# -------------------------------------------------------------
# 💡 嵌入 Hugo Page 說明
# 可直接用 iframe 嵌入不同 CSV，例如：
# <iframe src="https://npm-dataset.streamlit.app/?embed=true&csv=d01銅_s1.csv" width="100%" height="900" style="border:0;" loading="lazy"></iframe>
# 若要簡化，可在 Hugo 新增 shortcode：layouts/shortcodes/streamlit.html
# 內容：<iframe src="https://npm-dataset.streamlit.app/?embed=true&csv={{ .Get \"csv\" | urlquery }}" width="100%" height="900" style="border:0;" loading="lazy"></iframe>
# 使用：{{< streamlit csv="d02玉_s1.csv" >}}
# -------------------------------------------------------------

# === 護眼灰藍主題（柔和灰底＋淺灰藍主色） ===
import streamlit as st
st.markdown(
    """
    <style>
    body, .stApp {
        background-color: #F4F5F7 !important;
        color: #111827 !important;
    }
    div[data-testid="stDataFrame"] div[role="gridcell"] {
        background-color: #FAFBFD !important;
        color: #111827 !important;
        padding-top: 0.75rem !important;
        padding-bottom: 0.75rem !important;
    }
    div[data-testid="stDataFrame"] div[role="columnheader"] {
        background-color: #EEF2F7 !important;
        color: #111827 !important;
        padding-top: 0.75rem !important;
        padding-bottom: 0.75rem !important;
    }
    .stTextInput > div > div > input {
        background-color: #FFFFFF !important;
        color: #111827 !important;
    }
    .stSelectbox div[data-baseweb="select"] {
        background-color: #FFFFFF !important;
        color: #111827 !important;
    }
    .stDownloadButton button, .stButton button {
        background-color: #5A7BD8 !important; /* 改為淺灰藍 */
        color: white !important;
        border: none !important;
    }
    .stDownloadButton button:hover, .stButton button:hover {
        background-color: #7395EB !important; /* hover 更亮 */
        color: white !important;
    }
    .stExpander, .stSelectbox, .stTextInput, .stMultiSelect, .stDataFrame {
        border-radius: 10px !important;
        box-shadow: 0 1px 4px rgba(0,0,0,0.1);
    }
    </style>
    """,
    unsafe_allow_html=True
)

# streamlit_app.py —— 固定讀同層 CSV、無側欄、支援連結與縮圖、下拉分頁
import os
import math
import duckdb
import pandas as pd
import streamlit as st

st.set_page_config(page_title="CSV 典藏資料瀏覽器", layout="wide")
st.title("CSV 典藏資料瀏覽器")

# —— 本機快速切換測試檔（僅保留快速連結）——
TEST_FILES = [
    "d01銅_s1.csv", "d02玉_s1.csv", "d03瓷_s1.csv", "d04琺_s1.csv", "d05雜_s1.csv",
    "d06文_s1.csv", "d07織_s1.csv", "d08雕_s1.csv", "d09漆_s1.csv", "d10錢_s1.csv",
    "d20畫_s1.csv", "d21書_s1.csv", "d22帖_s1.csv", "d23扇_s1.csv", "d24絲_s1.csv"
]
import urllib.parse as _u

with st.expander("切換測試檔", expanded=True):
    # 僅顯示可點連結（已 URL encode）
    links = "　".join(f"[{name}](?csv={_u.quote(name)})" for name in TEST_FILES)
    st.markdown("快速連結：" + links, unsafe_allow_html=True)

# === 固定讀同層 CSV ===
CSV_NAME = "d01銅_s1.csv"
IMAGE_COL_OVERRIDE = "imageUrl_s"
CSV_PATH = os.path.join(os.path.dirname(__file__), CSV_NAME)

# DuckDB：以 scan 方式讀取，不一次載入全部記憶體
con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

# — URL 參數：?csv= 支援同層檔名或 http(s) 直連 —
try:
    _qp = st.query_params if hasattr(st, "query_params") else st.experimental_get_query_params()
except Exception:
    _qp = {}
_csv_param = _qp.get("csv", "")
if isinstance(_csv_param, list):
    _csv_param = _csv_param[0] if _csv_param else ""

if _csv_param:
    if str(_csv_param).lower().startswith(("http://", "https://")):
        _is_parquet = str(_csv_param).lower().endswith(".parquet")
        scan = f"parquet_scan('{_csv_param}')" if _is_parquet else f"read_csv_auto('{_csv_param}', SAMPLE_SIZE=200000)"
        source_hint = f"資料來源（URL）：{_csv_param}"
    else:
        _alt = os.path.join(os.path.dirname(__file__), _csv_param)
        if not os.path.exists(_alt):
            st.error(f"找不到指定的 csv 檔：{_alt}")
            st.stop()
        scan = f"read_csv_auto('{_alt}', SAMPLE_SIZE=200000)"
        source_hint = f"資料來源（同層檔案）：{_alt}"
else:
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
total = con.execute(f"SELECT COUNT(*) FROM {scan} WHERE {where}", params).fetchone()[0]
total_pages = max(1, math.ceil(total / page_size))

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

q = f"""
    SELECT {select_cols}
    FROM {scan}
    WHERE {where}
    LIMIT {int(page_size)} OFFSET {int(offset)}
"""
df = con.execute(q, params).fetchdf()

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

col_cfg = {}
if link_col:
    col_cfg[link_col] = st.column_config.LinkColumn(label=link_col, display_text="開啟連結")
if image_col:
    col_cfg[image_col] = st.column_config.ImageColumn(label=image_col, help="縮圖預覽")

st.data_editor(df, column_config=col_cfg, use_container_width=True, hide_index=True, disabled=True)

# 下載區
c1, c2 = st.columns(2)
with c1:
    st.caption("下載當頁 CSV")
    st.download_button("下載當頁", df.to_csv(index=False).encode("utf-8"), "page.csv", "text/csv")
with c2:
    st.caption("下載完整篩選 CSV")
    if st.button("產生完整 CSV"):
        out = "/tmp/filtered.csv"
        con.execute(f"COPY (SELECT {select_cols} FROM {scan} WHERE {where}) TO '{out}' (HEADER, DELIMITER ',')", params)
        with open(out, "rb") as f:
            st.download_button("下載完整結果", f, "filtered.csv", "text/csv")

st.divider()
st.caption(source_hint)
