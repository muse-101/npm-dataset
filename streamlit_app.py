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
st.set_page_config(page_title="CSV 典藏資料瀏覽器", layout="wide")
st.markdown(
    """
    <style>
    body, .stApp { background-color: #F4F5F7 !important; color: #111827 !important; }
    div[data-testid="stDataFrame"] div[role="gridcell"] {
        background-color: #FAFBFD !important; color: #111827 !important;
        padding-top: 0.75rem !important; padding-bottom: 0.75rem !important;
    }
    div[data-testid="stDataFrame"] div[role="columnheader"] {
        background-color: #EEF2F7 !important; color: #111827 !important;
        padding-top: 0.75rem !important; padding-bottom: 0.75rem !important;
    }
    .stTextInput > div > div > input { background-color: #FFFFFF !important; color: #111827 !important; }
    .stSelectbox div[data-baseweb="select"] { background-color: #FFFFFF !important; color: #111827 !important; }
    .stDownloadButton button, .stButton button { background-color: #5A7BD8 !important; color: #fff !important; border: none !important; }
    .stDownloadButton button:hover, .stButton button:hover { background-color: #7395EB !important; color: #fff !important; }
    .stExpander, .stSelectbox, .stTextInput, .stMultiSelect, .stDataFrame { border-radius: 10px !important; box-shadow: 0 1px 4px rgba(0,0,0,0.1); }
    /* 讓內層 App 在 iframe 裡撐滿高度，避免內容被壓縮 */
html, body, .stApp { min-height: 100% !important; height: auto !important; overflow: visible !important; }
#root, section.main, .main { min-height: 100% !important; height: auto !important; }
/* 嵌入模式去掉多餘上下內距，避免有效高度被吃掉 */
.block-container { min-height: calc(100% - 16px) !important; height: auto !important; padding-top: 1.25rem !important; padding-bottom: 1rem !important; }
    </style>
    """,
    unsafe_allow_html=True,
)

# ===== 主體 =====
import os
import math
import duckdb
import pandas as pd
from datetime import datetime
from urllib.parse import urlparse, unquote, urlunparse, quote

st.title("CSV 典藏資料瀏覽器")
src_ph = st.empty()

# === 固定讀同層 CSV（預設本地 d0.csv；可被 ?csv= 或側欄 URL 覆蓋） ===
CSV_NAME = "d0.csv"
IMAGE_COL_OVERRIDE = "imageUrl_s"
CSV_PATH = os.path.join(os.path.dirname(__file__), CSV_NAME)
DEFAULT_CSV_URL = ""  # 空字串 = 預設走本地 d0.csv
# ✅ 遠端 CSV 基底（用於 ?csv=僅給檔名時的後援解析），可用環境變數 CSV_BASE_URL 覆蓋
CSV_BASE_URL = os.environ.get("CSV_BASE_URL", "https://raw.githubusercontent.com/muse-101/npm-dataset/main/")
REMOTE_CSV_BASES = [CSV_BASE_URL]

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")
# 設定 User-Agent，避免部分遠端（含 GitHub Raw/CDN）拒絕空 UA
try:
    con.execute("SET http_user_agent='Mozilla/5.0 (Streamlit DuckDB)';")
except Exception:
    pass
# 更穩定的 httpfs 設定（遠端 CSV 讀取優化）
for _sql in [
    "SET enable_http_metadata_cache=true;",
    "SET http_keep_alive=true;",
    "SET http_max_redirects=10;",
    "SET http_open_timeout=30;",
    "SET http_reuse_connections=true;",
]:
    try:
        con.execute(_sql)
    except Exception:
        pass

# —— 左側欄：控制面板（快速連結 + URL 載入 + 欄位/搜尋/頁面大小 + 下載）——
import urllib.parse as _u
with st.sidebar:
    st.header("控制面板")

    # 本機快速切換：測試檔連結（可自行增修）
    TEST_FILES = [
        "d0.csv",
        "d01銅_s1.csv", "d02玉_s1.csv", "d03瓷_s1.csv", "d04琺_s1.csv", "d05雜_s1.csv",
        "d06文_s1.csv", "d07織_s1.csv", "d08雕_s1.csv", "d09漆_s1.csv", "d10錢_s1.csv",
        "d20畫_s1.csv", "d21書_s1.csv", "d22帖_s1.csv", "d23扇_s1.csv", "d24絲_s1.csv"
    ]
    def _display_name(fn: str) -> str: return fn[:-4] if fn.lower().endswith(".csv") else fn
    links = " | ".join([f"[{_display_name(n)}](?csv={_u.quote(n)})" for n in TEST_FILES])
    st.subheader("切換測試檔")
    st.markdown(links, unsafe_allow_html=True)
    st.markdown("---")

    # 直接貼 URL 載入（GitHub Raw / Google Drive uc?export=download / 任意 http/https）
    st.subheader("貼上 URL 載入")
    csv_url = st.text_input(
        "遠端資料網址（http/https）",
        value="",
        placeholder="例如：https://raw.githubusercontent.com/…/d01銅_s1.csv 或 https://…/file.parquet",
        key="csv_url_input",
    )
    col_u1, col_u2 = st.columns([1,1])
    with col_u1:
        if st.button("載入 URL", type="primary"):
            if csv_url.strip():
                url = csv_url.strip()
                try:
                    st.query_params.update({"csv": url})
                except Exception:
                    st.experimental_set_query_params(csv=url)
                st.rerun()
    with col_u2:
        if st.button("清除 URL"):
            try:
                st.query_params.clear()
            except Exception:
                st.experimental_set_query_params()
            st.rerun()

# === 解析網址參數（?csv= 可為 本地檔名 或 http/https URL） ===
RESOLVED_URL = None  # 若實際用到遠端網址，記錄在此供後援載入使用

def _normalize_drive_url(u: str) -> str:
    """將常見的 Google Drive 分享網址轉為可直接下載的 uc?export=download 形式。"""
    try:
        if "drive.google.com" not in u and "docs.google.com" not in u:
            return u
        import re
        m = re.search(r"/file/d/([a-zA-Z0-9_-]+)", u)
        if m:
            return f"https://drive.google.com/uc?export=download&id={m.group(1)}"
        m = re.search(r"[?&]id=([a-zA-Z0-9_-]+)", u)
        if m:
            return f"https://drive.google.com/uc?export=download&id={m.group(1)}"
        if "/uc?" in u:
            return u
    except Exception:
        pass
    return u

def _auto_encode_nonascii_url(u: str) -> str:
    """若 URL path 中含有非 ASCII（例如中文檔名），只編碼 path 部分，避免後端不認得。
    例如：https://raw.githubusercontent.com/.../d01銅_s1.csv -> path 會被 quote 成 %E9%8A%85
    """
    try:
        if not u or not isinstance(u, str) or not u.lower().startswith(("http://","https://")):
            return u
        pr = urlparse(u)
        # 只 re-encode path；query / fragment 保留（Hugo 端已處理）
        new_path = quote(pr.path, safe="/.-_~")
        if new_path == pr.path:
            return u
        return urlunparse((pr.scheme, pr.netloc, new_path, pr.params, pr.query, pr.fragment))
    except Exception:
        return u

        pr = urlparse(u)
        # 只 re-encode path；query / fragment 保留（已由 Hugo urlquery 處理）
        new_path = quote(pr.path, safe="/.-_~")  # 常用安全字保留
        if new_path == pr.path:
            return u
        return urlunparse((pr.scheme, pr.netloc, new_path, pr.params, pr.query, pr.fragment))
    except Exception:
        return u
        # 形式1：/file/d/<id>/view
        import re
        m = re.search(r"/file/d/([a-zA-Z0-9_-]+)", u)
        if m:
            return f"https://drive.google.com/uc?export=download&id={m.group(1)}"
        # 形式2：open?id=<id>
        m = re.search(r"[?&]id=([a-zA-Z0-9_-]+)", u)
        if m:
            return f"https://drive.google.com/uc?export=download&id={m.group(1)}"
        # 若本來就是 uc? 形式，直接回傳
        if "/uc?" in u:
            return u
    except Exception:
        pass
    return u
try:
    _qp = st.query_params if hasattr(st, "query_params") else st.experimental_get_query_params()
except Exception:
    _qp = {}
_csv_param = _qp.get("csv", "")
if isinstance(_csv_param, list):
    _csv_param = _csv_param[0] if _csv_param else ""

if _csv_param:
    if str(_csv_param).lower().startswith(("http://", "https://")):
        _url = _auto_encode_nonascii_url(_normalize_drive_url(str(_csv_param)))
        _is_parquet = _url.lower().endswith(".parquet")
        scan = f"parquet_scan('{_url}')" if _is_parquet else f"read_csv_auto('{_url}', SAMPLE_SIZE=200000)"
        source_hint = f"資料來源（URL）：{_url}"
        RESOLVED_URL = _url
    else:
        _alt = os.path.join(os.path.dirname(__file__), _csv_param)
        if os.path.exists(_alt):
            scan = f"read_csv_auto('{_alt}', SAMPLE_SIZE=200000)"
            source_hint = f"資料來源（同層檔案）：{_alt}"
        else:
            # 後援：將 csv 檔名接到遠端基底（例如 GitHub Raw），支援中文檔名
            enc = _u.quote(_csv_param)
            fallback_url = REMOTE_CSV_BASES[0].rstrip('/') + '/' + enc
            scan = f"read_csv_auto('{fallback_url}', SAMPLE_SIZE=200000)"
            source_hint = f"資料來源（遠端後援）：{fallback_url}"
else:
    if DEFAULT_CSV_URL:
        _url = _auto_encode_nonascii_url(_normalize_drive_url(DEFAULT_CSV_URL))
        scan = f"read_csv_auto('{_url}', SAMPLE_SIZE=200000)"
        source_hint = f"資料來源（GitHub Raw）：{_url}"
        RESOLVED_URL = _url
    else:
        scan = f"read_csv_auto('{CSV_PATH}', SAMPLE_SIZE=200000)"
        source_hint = f"資料來源：{CSV_PATH}"

# === 下載檔名：muz01_XXXX_OOOO.csv ===
# XXXX = 原始資料檔名（無副檔名），OOOO = 目前時間戳（YYYYMMDD_HHMMSS）

def _infer_src_basename(src: str) -> str:
    try:
        if str(src).lower().startswith(("http://","https://")):
            last = os.path.basename(urlparse(src).path)
            return os.path.splitext(unquote(last))[0] or "data"
        else:
            return os.path.splitext(os.path.basename(src))[0]
    except Exception:
        return "data"

_src_for_name = _csv_param or (DEFAULT_CSV_URL if DEFAULT_CSV_URL else CSV_PATH)
SRC_BASENAME = _infer_src_basename(_src_for_name)
TS = datetime.now().strftime("%Y%m%d_%H%M%S")
DL_NAME = f"muz01_{SRC_BASENAME}_{TS}.csv"
DL_STEM = DL_NAME[:-4] if DL_NAME.lower().endswith('.csv') else DL_NAME

src_ph.caption(source_hint)
st.success("目前預設載入本地檔案 d0.csv，可用 ?csv= 或側欄貼上 URL 變更來源。")

# === 偵錯區（?debug=1 時顯示解析後參數） ===
try:
    _dbg = _qp.get("debug", "")
    _dbg = _dbg[0] if isinstance(_dbg, list) else _dbg
    if str(_dbg) in ("1","true","yes"):
        st.info({
            "csv_param_raw": _csv_param,
            "resolved_url": RESOLVED_URL,
            "source_hint": source_hint,
        })
except Exception:
    pass

# === 預覽欄位（以便生成 UI） ===
try:
    preview = con.execute(f"SELECT * FROM {scan} LIMIT 1").fetchdf()
except Exception as e:
    # 後援：對遠端 URL 嘗試用 pandas 載入，再註冊成 DuckDB 臨時 view
    if RESOLVED_URL:
        try:
            if RESOLVED_URL.lower().endswith('.parquet'):
                _df_all = pd.read_parquet(RESOLVED_URL)
            else:
                _df_all = pd.read_csv(RESOLVED_URL)
            con.register('remote_fallback', _df_all)
            scan = 'remote_fallback'
            source_hint = source_hint + "（pandas 後援載入）"
            preview = con.execute(f"SELECT * FROM {scan} LIMIT 1").fetchdf()
        except Exception as ee:
            st.error(f"""讀取資料結構失敗：{e}
遠端後援也失敗：{ee}
請確認連結是否為可直接下載的原始檔（GitHub Raw / Drive uc?export=download）。""")
            st.stop()
    else:
        st.error(f"讀取資料結構失敗：{e}")
        st.stop()

cols = preview.columns.tolist()
if not cols:
    st.error("CSV 沒有欄位。")
    st.stop()

# 🔎 驗證必備欄位：sk1、sk2、sk3（表格仍可瀏覽；Sankey/sk節點缺則提示）
REQUIRED_SK = ["sk1", "sk2", "sk3"]
missing_sk = [c for c in REQUIRED_SK if c not in cols]
if missing_sk:
    st.warning("""本 CSV 缺少必備欄位：""" + ", ".join(missing_sk) + """。
表格仍可瀏覽，但「sk節點」與「Sankey」將無法正確顯示；請補上 sk1、sk2、sk3 三欄。""")

# === 側欄：欄位與搜尋 / 每頁筆數 ===
with st.sidebar:
    st.subheader("欄位與搜尋")
    show_cols = st.multiselect("顯示欄位", cols, default=cols[: min(10, len(cols))])
    kw_cols   = st.multiselect("關鍵字搜尋欄位", cols, default=show_cols or cols)
    page_size = st.selectbox("每頁筆數", [25, 50, 100, 200, 500], index=2)
    st.markdown("---")

# === 搜尋條件 ===
kw_value = st.session_state.get("keyword", "")
where = "TRUE"
params = {}
if kw_value and kw_cols:
    like_parts = [f'CAST("{c}" AS TEXT) ILIKE $kw' for c in kw_cols]
    where = "(" + " OR ".join(like_parts) + ")"
    params["kw"] = f"%{kw_value}%"

# === 計數（搜尋後 / 原始基準） ===
try:
    total = con.execute(f"SELECT COUNT(*) FROM {scan} WHERE {where}", params).fetchone()[0]
    base_total = con.execute(f"SELECT COUNT(*) FROM {scan}").fetchone()[0]
except Exception as e:
    st.error(f"計數失敗：{e}")
    st.stop()

total_pages = max(1, math.ceil(total / page_size))
base_pages  = max(1, math.ceil(base_total / page_size))

# === 分頁控制 ===
if "page" not in st.session_state or st.session_state.page < 1 or st.session_state.page > total_pages:
    st.session_state.page = 1
b1, b2, b3, b4 = st.columns(4)
with b1:
    if st.button("⏮ 第一頁"): st.session_state.page = 1
with b2:
    if st.button("◀ 上一頁") and st.session_state.page > 1: st.session_state.page -= 1
with b3:
    if st.button("下一頁 ▶") and st.session_state.page < total_pages: st.session_state.page += 1
with b4:
    if st.button("最後一頁 ⏭"): st.session_state.page = total_pages
page = st.session_state.page

# === 查詢當頁 ===
offset = (page - 1) * page_size
select_cols_sql = ", ".join([f'"{c}"' for c in (show_cols or cols)])
q_page = f"""
    SELECT {select_cols_sql}
    FROM {scan}
    WHERE {where}
    LIMIT {int(page_size)} OFFSET {int(offset)}
"""
try:
    df_page = con.execute(q_page, params).fetchdf()
except Exception as e:
    st.error(f"讀取頁面資料失敗：{e}")
    st.stop()

# === 自動辨識連結欄／圖片欄（表格用） ===
def find_col(df: pd.DataFrame, candidates):
    cands = {c.lower() for c in candidates}
    for c in df.columns:
        if c.lower() in cands:
            return c
    return None

link_col  = find_col(df_page, {"url", "link", "api_link", "href"})
image_override = IMAGE_COL_OVERRIDE if (IMAGE_COL_OVERRIDE and IMAGE_COL_OVERRIDE in df_page.columns) else None
image_col = image_override or find_col(df_page, {"imageurl","image_url","imageurl_s","thumb","thumbnail","img","image"})
col_cfg = {}
if link_col:
    col_cfg[link_col] = st.column_config.LinkColumn(label=link_col, display_text="開啟連結")
if image_col:
    col_cfg[image_col] = st.column_config.ImageColumn(label=image_col, help="縮圖預覽")

# ================= Tabs：表格 / sk節點 / Sankey =================
tab_table, tab_nodes, tab_sankey = st.tabs(["📊 表格", "🔖 sk節點", "🪢 Sankey"]) 

with tab_table:
    st.subheader("資料表（當頁）")
    # 把搜尋輸入與統計移到表格分頁
    keyword = st.text_input("關鍵字（ILIKE 模糊搜尋）", value=kw_value, key="keyword")
    if kw_value and kw_cols:
        st.write(f"符合條件：{total:,} 筆；第 {page} / {total_pages} 頁  ({base_total:,} 筆；第 1 / {base_pages} 頁)")
    else:
        st.write(f"符合條件：{total:,} 筆；第 {page} / {total_pages} 頁")
    st.data_editor(df_page, column_config=col_cfg, use_container_width=True, hide_index=True, disabled=True)

with tab_nodes:
    st.subheader("sk 節點（不套用搜尋 / 不分頁，固定顯示 id, sk1, sk2, sk3）")
    if missing_sk:
        st.error("此 CSV 不包含 sk1、sk2、sk3 三欄，無法顯示 sk 節點表。請補齊後再試。")
    else:
        # 讀取完整資料（忽略 WHERE 與分頁），僅取需要的欄位
        base_cols = ["sk1", "sk2", "sk3"]
        cols_exist = [c for c in ["id"] + base_cols if c in cols]
        sel_cols_nodes = ", ".join([f'"{c}"' for c in cols_exist])
        q_nodes = f"SELECT {sel_cols_nodes} FROM {scan}"
        df_nodes_full = con.execute(q_nodes).fetchdf().fillna("（缺值）")
        if "id" not in df_nodes_full.columns:
            df_nodes_full.insert(0, "id", "")
        df_nodes_full = df_nodes_full[["id", "sk1", "sk2", "sk3"]]

        view = st.radio("檢視方式", ["原始列（id, sk1, sk2, sk3）", "唯一組合 + 計數（sk1, sk2, sk3）"], horizontal=True)
        if view.startswith("唯一"):
            df_nodes = (
                df_nodes_full[["sk1","sk2","sk3"]]
                .value_counts(["sk1","sk2","sk3"])  # pandas >= 1.4
                .rename("count").reset_index()
                .sort_values("count", ascending=False)
            )
        else:
            df_nodes = df_nodes_full

        st.data_editor(df_nodes, use_container_width=True, hide_index=True, disabled=True)

        cna1, cna2 = st.columns(2)
        with cna1:
            st.download_button("下載 sk 節點（當前檢視）", data=df_nodes.to_csv(index=False).encode("utf-8-sig"), file_name=DL_STEM + "_nodes.csv", mime="text/csv")
        with cna2:
            st.download_button("下載 sk 原始（id, sk1, sk2, sk3）", data=df_nodes_full.to_csv(index=False).encode("utf-8-sig"), file_name=DL_STEM + "_raw.csv", mime="text/csv")

with tab_sankey:
    st.subheader("Sankey（固定使用 sk1 → sk2 → sk3；不套用搜尋、不分頁）")
    if missing_sk:
        st.error("此 CSV 不包含 sk1、sk2、sk3 三欄，無法繪製 Sankey。請補齊後再試。")
    else:
        try:
            import plotly.graph_objects as go
        except ModuleNotFoundError:
            st.error("""找不到 Plotly。請先安裝：`pip install plotly` 或 `pip3 install plotly`。若用 Conda：`conda install -c plotly plotly`。
（Tabs 已顯示；安裝後重啟即可顯示 Sankey）""")
        else:
            # 取與「sk節點」一致的資料來源（完整、不分頁、不搜尋）
            q_nodes_for_sankey = f"SELECT \"sk1\", \"sk2\", \"sk3\" FROM {scan}"
            df_nodes_for_sankey = con.execute(q_nodes_for_sankey).fetchdf().fillna("（缺值）")
            work = df_nodes_for_sankey[["sk1","sk2","sk3"]].copy()

            # 建立 links（sk1→sk2、sk2→sk3）
            links = []
            for a, b in [("sk1","sk2"),("sk2","sk3")]:
                g = work.groupby([a,b], dropna=False, as_index=False).size()
                g.rename(columns={"size":"value", a:"src", b:"dst"}, inplace=True)
                links.append(g)
            links_df = pd.concat(links, ignore_index=True)

            vmax = int(max(1, int(links_df["value"].max())))
            min_val = st.slider("過濾：最小權重", 1, vmax, value=1)
            links_df = links_df[links_df["value"] >= min_val]

            if links_df.empty:
                st.info("過濾後沒有連線可顯示，請放寬門檻或更換資料條件。")
            else:
                labels = pd.unique(pd.concat([work["sk1"], work["sk2"], work["sk3"]], ignore_index=True)).tolist()
                idx = {lab: i for i, lab in enumerate(labels)}
                links_df = links_df.assign(
                    source_id=links_df["src"].map(idx),
                    target_id=links_df["dst"].map(idx),
                )

                fig = go.Figure(data=[go.Sankey(
                    node=dict(
                        label=labels,
                        pad=20,
                        thickness=18,
                        color="#FFFFFF",
                        line=dict(color="rgba(0,0,0,0)", width=0),
                    ),
                    link=dict(
                        source=links_df["source_id"],
                        target=links_df["target_id"],
                        value=links_df["value"],
                        color="rgba(90,123,216,0.18)",
                    ),
                    textfont=dict(color="#0B1220", size=16, family="Microsoft JhengHei, Heiti TC, sans-serif"),
                )])
                fig.update_layout(
                    title_text="Sankey：sk1 → sk2 → sk3",
                    font_size=16,
                    font=dict(family="Microsoft JhengHei, Heiti TC, sans-serif", color="#0B1220"),
                    paper_bgcolor="#FFFFFF",
                    plot_bgcolor="#FFFFFF",
                    margin=dict(l=10, r=10, t=40, b=10),
                    hoverlabel=dict(bgcolor="#FFFFFF", font_size=14, font_family="Microsoft JhengHei, Heiti TC, sans-serif"),
                )
                st.plotly_chart(fig, use_container_width=True)

# —— 側欄：下載區 ——
with st.sidebar:
    st.subheader("下載")
    st.caption("下載當前頁面或完整篩選結果")
    st.download_button("下載當頁", df_page.to_csv(index=False).encode("utf-8"), DL_NAME, "text/csv")
    if st.button("產生完整 CSV"):
        out = "/tmp/filtered.csv"
        con.execute(
            f"COPY (SELECT {select_cols_sql} FROM {scan} WHERE {where}) TO '{out}' (HEADER, DELIMITER ',')",
            params,
        )
        with open(out, "rb") as f:
            st.download_button("下載完整結果", f, DL_NAME, "text/csv")

# ===（進階）Hugo 自適應高度：從 App 回傳內容高度給父頁 ===
# 說明：Firefox 在 iframe 內對 vh 計算較嚴格，建議改成 parent <-> iframe 的 postMessage 通訊
# 用法：
#  1) 本段會定期把 document 高度回傳給 parent；
#  2) 請在 Hugo shortcode 加上 window.addEventListener('message', ...) 接收並設定對應 iframe 的 style.height。
try:
    from streamlit.components.v1 import html as _html
    _is_embed = str((_qp.get("embed") if not isinstance(_qp.get("embed"), list) else _qp.get("embed")[0]) or "").lower() == "true"
    if _is_embed:
        _html(
            """
            <script>
            (function(){
              function sendSize(){
                var h = Math.max(document.documentElement.scrollHeight, document.body.scrollHeight);
                try{ parent.postMessage({ kind: 'sl-size', height: h }, '*'); }catch(e){}
              }
              var ro = new ResizeObserver(function(){ sendSize(); });
              ro.observe(document.documentElement);
              window.addEventListener('load', sendSize);
              setInterval(sendSize, 800);
              // 初次觸發
              sendSize();
            })();
            </script>
            """,
            height=0,
            width=0,
        )
except Exception:
    pass
