# streamlit_app.py —— 固定讀同層 CSV、無側欄、支援連結與縮圖、下拉分頁
import os
import math
import duckdb
import pandas as pd
import streamlit as st

st.set_page_config(page_title="CSV 典藏資料瀏覽器", layout="wide")
st.title("CSV 典藏資料瀏覽器")

# 介面：柔和灰主題切換（與表格一致的灰調，降低對比）
use_gray = st.toggle("柔和灰主題", value=True, help="啟用全站柔和灰，含表格背景，降低白底對比")

LIGHT = {
    "bg": "#FFFFFF",
    "bg2": "#F7F8FA",
    "text": "#1F2937",
    "primary": "#1E90FF",
    "chip_bg": "#EAF4FF",
    "chip_text": "#124C9E",
    "shadow": "rgba(30,144,255,0.28)",
    "table_bg": "#FFFFFF",
    "table_head": "#F3F6FA",
    "table_border": "#E2E8F0",
}
GRAY = {
    "bg": "#E6E8EC",      # 頁面底色：柔和灰（再深一階）
    "bg2": "#E1E5EB",     # 區塊/expander：略淺灰（再深一階）
    "text": "#0F172A",    # 深灰文字（微增對比）
    "primary": "#1D4ED8", # 主色改為墨藍
    "chip_bg": "#DAE8FF",
    "chip_text": "#0B2E73",
    "shadow": "rgba(37,99,235,0.32)",
    "table_bg": "#F1F4F8",   # 表格底色再深一階
    "table_head": "#E6ECF3", # 表頭更深一階
    "table_border": "#C6D0DC",
}
P = GRAY if use_gray else LIGHT

# 固定主色為「50% 深淺」的藍（介於 #93C5FD 與 #1D4ED8 之間）
# 計算結果：約 #5889EA
P['primary'] = '#5889EA'
P['shadow']  = 'rgba(88,137,234,0.28)'
P['chip_text'] = '#0B2E73'
P['chip_bg']   = '#DAE8FF'

# 自訂樣式：Multiselect/標籤、整體背景、表格底色統一灰調
st.markdown(
    f"""
    <style>
    /* 全域背景與文字 */
    .stApp {{ background: {P['bg']}; color: {P['text']}; }}
    .stApp .block-container {{ background: {P['bg']}; }}

    /* 區塊/expander 背景 */
    details {{ background: {P['bg2']}; border-radius: 10px; padding: .35rem .5rem; border: 1px solid {P['table_border']}; }}
    details > summary {{ color: {P['text']}; font-weight: 600; }}

    /* DataFrame / DataEditor 表格配色（避免純白）*/
    .stDataFrame, .stDataEditor {{ background: {P['table_bg']} !important; }}
    .stDataEditor table, .stDataFrame table {{ background: {P['table_bg']} !important; color: {P['text']} !important; }}
    .stDataEditor th, .stDataFrame th {{ background: {P['table_head']} !important; color: {P['text']} !important; }}
    .stDataEditor td, .stDataFrame td {{ background: {P['table_bg']} !important; color: {P['text']} !important; border-color: {P['table_border']} !important; }}

    /* 表格外框/滾動區域 */
    .stDataEditor div[role="grid"], .stDataFrame div[role="grid"] {{ border: 1px solid {P['table_border']} !important; border-radius: 8px; }}

    /* Multiselect 外框預設、hover、focus 改藍，背景跟著灰 */
    .stApp div[data-baseweb="select"] > div {{
      border: 1px solid {P['primary']} !important; box-shadow: none !important;
      background: {P['bg2']}; color: {P['text']};
    }}
    .stApp div[data-baseweb="select"] > div:hover {{ border-color: {P['primary']} !important; }}
    .stApp div[data-baseweb="select"] > div:focus-within {{
      border-color: {P['primary']} !important; box-shadow: 0 0 0 2px {P['shadow']} !important;
    }}

    /* 已選 chips 樣式 */
    .stApp div[data-baseweb="tag"] {{ background: {P['chip_bg']} !important; color: {P['chip_text']} !important; border: 1px solid {P['primary']} !important; }}
    .stApp div[data-baseweb="tag"] span {{ color: {P['chip_text']} !important; }}

    /* 按鈕顏色微調（灰主題下依然藍邊） */
    .stButton>button {{ background: {P['primary']}1A; color: {P['text']}; border: 1px solid {P['primary']}; }}
    .stButton>button:hover {{ background: {P['primary']}2B; }}

    /* 分隔線 */
    hr {{ border-color: {P['table_border']} !important; opacity: .8; }}
    </style>
    """,
    unsafe_allow_html=True,
)

# === 固定讀同層 CSV（依需求修改檔名） ===
CSV_NAME = "d01銅_s1.csv"   # ← 將此名稱改為你要讀取的檔名
# 可選：指定圖片欄位覆蓋（例如你現在的 imageUrl_s）
IMAGE_COL_OVERRIDE = "imageUrl_s"  # 沒有就設為空字串 ""
CSV_PATH = os.path.join(os.path.dirname(__file__), CSV_NAME)
if not os.path.exists(CSV_PATH):
    st.error(f"找不到 {CSV_PATH}，請把 CSV 放在與本檔相同的資料夾，或修改 CSV_NAME。")
    st.stop()

# DuckDB：以 scan 方式讀取，不一次載入全部記憶體
con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")
scan = f"read_csv_auto('{CSV_PATH}', SAMPLE_SIZE=200000)"
source_hint = f"資料來源：{CSV_PATH}"

# 先抓欄位
try:
    preview = con.execute(f"SELECT * FROM {scan} LIMIT 1").fetchdf()
except Exception as e:
    st.error(f"讀取資料結構失敗：{e}")
    st.stop()

cols = preview.columns.tolist()
if not cols:
    st.error("CSV 沒有欄位。")
    st.stop()

# === 上方控制列：欄位、搜尋、每頁筆數（下拉） ===
with st.expander("欄位與搜尋", expanded=True):
    show_cols = st.multiselect("顯示欄位", cols, default=cols[: min(10, len(cols))])
    kw_cols   = st.multiselect("關鍵字搜尋欄位", cols, default=show_cols or cols)
    keyword   = st.text_input("關鍵字（ILIKE 模糊搜尋）", "")
    page_size = st.selectbox("每頁筆數", [25, 50, 100, 200, 500], index=2)

# WHERE 條件
where = "TRUE"
params = {}
if keyword and kw_cols:
    like_parts = [f'CAST("{c}" AS TEXT) ILIKE $kw' for c in kw_cols]
    where = "(" + " OR ".join(like_parts) + ")"
    params["kw"] = f"%{keyword}%"

# 先算總筆數，再給頁碼下拉
try:
    total = con.execute(f"SELECT COUNT(*) FROM {scan} WHERE {where}", params).fetchone()[0]
except Exception as e:
    st.error(f"統計總筆數時出錯：{e}")
    st.stop()

total_pages = max(1, math.ceil(total / page_size))

# 以四個按鈕控制頁碼：⏮ 第一頁、◀ 上一頁、下一頁 ▶、最後一頁 ⏭
if "page" not in st.session_state or st.session_state.page < 1 or st.session_state.page > total_pages:
    st.session_state.page = 1

b1, b2, b3, b4 = st.columns(4)
with b1:
    if st.button("⏮ 第一頁"):
        st.session_state.page = 1
with b2:
    if st.button("◀ 上一頁") and st.session_state.page > 1:
        st.session_state.page -= 1
with b3:
    if st.button("下一頁 ▶") and st.session_state.page < total_pages:
        st.session_state.page += 1
with b4:
    if st.button("最後一頁 ⏭"):
        st.session_state.page = total_pages

page = st.session_state.page
st.caption(f"第 {page} / {total_pages} 頁")

# 查詢當頁資料
offset = (page - 1) * page_size
select_cols = ", ".join([f'"{c}"' for c in (show_cols or cols)])

try:
    q = f"""
        SELECT {select_cols}
        FROM {scan}
        WHERE {where}
        LIMIT {int(page_size)} OFFSET {int(offset)}
    """
    df = con.execute(q, params).fetchdf()
except Exception as e:
    st.error(f"讀取當頁資料時出錯：{e}")
    st.stop()

st.write(f"符合條件：{total:,} 筆；第 {page} / {total_pages} 頁")

# === 自動辨識連結欄／圖片欄 ===
def find_col(df, candidates):
    cands = {c.lower() for c in candidates}
    for c in df.columns:
        if c.lower() in cands:
            return c
    return None

link_col  = find_col(df, {"url", "link", "api_link", "href"})
image_col = IMAGE_COL_OVERRIDE if IMAGE_COL_OVERRIDE and IMAGE_COL_OVERRIDE in df.columns else \
    find_col(df, {"imageurl","image_url","imageurl_s","thumb","thumbnail","img","image"})

# 使用 data_editor：讓 link 可點、image 顯示縮圖
col_cfg = {}
if link_col:
    col_cfg[link_col] = st.column_config.LinkColumn(
        label=link_col,
        display_text="開啟連結"
    )
if image_col:
    col_cfg[image_col] = st.column_config.ImageColumn(
        label=image_col,
        help="縮圖預覽"
    )

st.data_editor(
    df,
    column_config=col_cfg,
    use_container_width=True,
    hide_index=True,
    disabled=True  # 僅瀏覽，不允許編輯
)

# 下載區
c1, c2 = st.columns(2)
with c1:
    st.caption("下載當頁 CSV")
    st.download_button("下載當頁", df.to_csv(index=False).encode("utf-8"), "page.csv", "text/csv")
with c2:
    st.caption("下載完整篩選 CSV")
    if st.button("產生完整 CSV"):
        out = "/tmp/filtered.csv"
        con.execute(
            f"COPY (SELECT {select_cols} FROM {scan} WHERE {where}) TO '{out}' (HEADER, DELIMITER ',')",
            params
        )
        with open(out, "rb") as f:
            st.download_button("下載完整結果", f, "filtered.csv", "text/csv")


# 來源資訊放在最下方
st.divider()
st.caption(source_hint)
