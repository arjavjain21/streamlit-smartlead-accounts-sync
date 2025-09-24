
import os
import json
import time
import httpx
import pandas as pd
import streamlit as st
from psycopg_pool import ConnectionPool

DEFAULT_ENDPOINT = os.environ.get(
    "SMARTLEAD_ENDPOINT",
    "https://server.smartlead.ai/api/email-account/get-total-email-accounts"
)
DEFAULT_LIMIT = int(os.environ.get("SMARTLEAD_LIMIT", "10000"))
REQUEST_TIMEOUT = int(os.environ.get("SMARTLEAD_TIMEOUT", "60"))
SUPABASE_DB_URL = os.environ.get("SUPABASE_DB_URL", "").strip()
TABLE_NAME = "public.all_accounts_realtime"

if not SUPABASE_DB_URL:
    st.error("SUPABASE_DB_URL is not set. Set it in Streamlit Secrets or environment.")
    st.stop()

@st.cache_resource
def get_pool():
    # Disable server-side prepares due to Supabase transaction pooler
    return ConnectionPool(
        conninfo=SUPABASE_DB_URL,
        min_size=1,
        max_size=4,
        timeout=10,
        kwargs={
            "autocommit": True,
            "row_factory": dict_row,
            "prepare_threshold": 0,
            "simple_query_protocol": True,
        }
    )

pool = get_pool()

# --- DDL statements executed separately ---
DDL_STATEMENTS = [
    """
    create table if not exists public.app_settings (
      key text primary key,
      value text,
      updated_at timestamptz default now()
    )
    """,
    """
    create table if not exists public.sync_runs (
      id bigserial primary key,
      started_at timestamptz default now(),
      finished_at timestamptz,
      ok boolean,
      message text,
      rows_upserted integer
    )
    """,
    """
    create table if not exists public.all_accounts_realtime (
      id bigint not null,
      time_to_wait_in_mins text null,
      from_name text null,
      from_email text null,
      __typename text null,
      type text null,
      smtp_host text null,
      is_smtp_success boolean null,
      is_imap_success boolean null,
      message_per_day bigint null,
      daily_sent_count text null,
      smart_sender_flag text null,
      client_id text null,
      client text null,
      "isSPFVerified" text null,
      "isDKIMVerified" text null,
      "isDMARCVerified" text null,
      "lastVerifiedTime" text null,
      warmup_status text null,
      warmup_reputation text null,
      is_warmup_blocked text null,
      tag_id text null,
      tag_name text null,
      tag_color text null,
      email_account_tag_mappings_count text null,
      email_campaign_account_mappings_count text null,
      constraint all_accounts_realtime_pkey primary key (id)
    )
    """
]

with pool.connection() as conn, conn.transaction(), conn.cursor() as cur:
    for stmt in DDL_STATEMENTS:
        cur.execute(stmt)

def db_get_bearer() -> str | None:
    with pool.connection() as conn, conn.cursor() as cur:
        cur.execute("select value from public.app_settings where key = 'smartlead_bearer'")
        row = cur.fetchone()
        return row[0] if row else None

def db_set_bearer(token: str) -> None:
    with pool.connection() as conn, conn.transaction(), conn.cursor() as cur:
        cur.execute("""
            insert into public.app_settings (key, value, updated_at)
            values ('smartlead_bearer', %s, now())
            on conflict (key) do update set value = excluded.value, updated_at = now();
        """, (token,))

def db_clear_bearer() -> None:
    with pool.connection() as conn, conn.transaction(), conn.cursor() as cur:
        cur.execute("delete from public.app_settings where key = 'smartlead_bearer'")

def resolve_bearer() -> str | None:
    token = db_get_bearer()
    if token:
        return token.strip()
    env_token = os.environ.get("SMARTLEAD_BEARER")
    if env_token:
        return env_token.strip()
    return st.session_state.get("bearer")

def to_text(v):
    if v is None:
        return None
    return str(v)

def extract_dns_bits(dns: dict):
    if not isinstance(dns, dict):
        return None, None, None, None
    return (
        to_text(dns.get("isSPFVerified")),
        to_text(dns.get("isDKIMVerified")),
        to_text(dns.get("isDMARCVerified")),
        to_text(dns.get("lastVerifiedTime"))
    )

def flatten_tags(mappings: list):
    tag_ids, tag_names, tag_colors = [], [], []
    for m in mappings or []:
        t = m.get("tag") or {}
        if "id" in t: tag_ids.append(str(t.get("id")))
        if "name" in t: tag_names.append(t.get("name"))
        if "color" in t: tag_colors.append(t.get("color"))
    return ",".join(tag_ids) or None, ",".join(tag_names) or None, ",".join(tag_colors) or None

UPSERT_SQL = f"""
insert into {TABLE_NAME} (
  id, time_to_wait_in_mins, from_name, from_email, __typename, type, smtp_host,
  is_smtp_success, is_imap_success, message_per_day, daily_sent_count,
  smart_sender_flag, client_id, client, "isSPFVerified", "isDKIMVerified",
  "isDMARCVerified", "lastVerifiedTime", warmup_status, warmup_reputation,
  is_warmup_blocked, tag_id, tag_name, tag_color,
  email_account_tag_mappings_count, email_campaign_account_mappings_count
)
values (
  %(id)s, %(time_to_wait_in_mins)s, %(from_name)s, %(from_email)s, %(__typename)s, %(type)s, %(smtp_host)s,
  %(is_smtp_success)s, %(is_imap_success)s, %(message_per_day)s, %(daily_sent_count)s,
  %(smart_sender_flag)s, %(client_id)s, %(client)s, %(isSPFVerified)s, %(isDKIMVerified)s,
  %(isDMARCVerified)s, %(lastVerifiedTime)s, %(warmup_status)s, %(warmup_reputation)s,
  %(is_warmup_blocked)s, %(tag_id)s, %(tag_name)s, %(tag_color)s,
  %(email_account_tag_mappings_count)s, %(email_campaign_account_mappings_count)s
)
on conflict (id) do update set
  time_to_wait_in_mins = EXCLUDED.time_to_wait_in_mins,
  from_name = EXCLUDED.from_name,
  from_email = EXCLUDED.from_email,
  __typename = EXCLUDED.__typename,
  type = EXCLUDED.type,
  smtp_host = EXCLUDED.smtp_host,
  is_smtp_success = EXCLUDED.is_smtp_success,
  is_imap_success = EXCLUDED.is_imap_success,
  message_per_day = EXCLUDED.message_per_day,
  daily_sent_count = EXCLUDED.daily_sent_count,
  smart_sender_flag = EXCLUDED.smart_sender_flag,
  client_id = EXCLUDED.client_id,
  client = EXCLUDED.client,
  "isSPFVerified" = EXCLUDED."isSPFVerified",
  "isDKIMVerified" = EXCLUDED."isDKIMVerified",
  "isDMARCVerified" = EXCLUDED."isDMARCVerified",
  "lastVerifiedTime" = EXCLUDED."lastVerifiedTime",
  warmup_status = EXCLUDED.warmup_status,
  warmup_reputation = EXCLUDED.warmup_reputation,
  is_warmup_blocked = EXCLUDED.is_warmup_blocked,
  tag_id = EXCLUDED.tag_id,
  tag_name = EXCLUDED.tag_name,
  tag_color = EXCLUDED.tag_color,
  email_account_tag_mappings_count = EXCLUDED.email_account_tag_mappings_count,
  email_campaign_account_mappings_count = EXCLUDED.email_campaign_account_mappings_count
;
"""

def upsert_accounts(rows: list[dict]) -> int:
    if not rows:
        return 0
    with pool.connection() as conn, conn.transaction(), conn.cursor() as cur:
        cur.executemany(UPSERT_SQL, rows)
    return len(rows)

def fetch_all_accounts(bearer: str, endpoint: str, limit: int) -> list[dict]:
    headers = {"Accept": "application/json", "Authorization": f"Bearer {bearer.strip()}"}
    all_rows, offset = [], 0
    with httpx.Client(timeout=REQUEST_TIMEOUT) as client:
        while True:
            r = client.get(endpoint, headers=headers, params={"offset": offset, "limit": limit})
            if r.status_code == 401:
                raise RuntimeError("401 Unauthorized. Update your Bearer token.")
            r.raise_for_status()
            payload = r.json()
            accounts = (payload.get("data") or {}).get("email_accounts") or []
            for a in accounts:
                dns = a.get("dns_validation_status") or {}
                isSPF, isDKIM, isDMARC, lastV = extract_dns_bits(dns)
                tags = a.get("email_account_tag_mappings") or []
                tag_id, tag_name, tag_color = flatten_tags(tags)
                agg = (a.get("email_campaign_account_mappings_aggregate") or {}).get("aggregate", {})
                row = {
                    "id": a.get("id"),
                    "time_to_wait_in_mins": to_text(a.get("time_to_wait_in_mins")),
                    "from_name": a.get("from_name"),
                    "from_email": a.get("from_email"),
                    "__typename": a.get("__typename"),
                    "type": a.get("type"),
                    "smtp_host": a.get("smtp_host"),
                    "is_smtp_success": a.get("is_smtp_success"),
                    "is_imap_success": a.get("is_imap_success"),
                    "message_per_day": a.get("message_per_day"),
                    "daily_sent_count": to_text(a.get("daily_sent_count")),
                    "smart_sender_flag": a.get("smart_sender_flag"),
                    "client_id": to_text(a.get("client_id")),
                    "client": to_text(a.get("client")),
                    "isSPFVerified": isSPF,
                    "isDKIMVerified": isDKIM,
                    "isDMARCVerified": isDMARC,
                    "lastVerifiedTime": lastV,
                    "warmup_status": to_text((a.get("email_warmup_details") or {}).get("status")),
                    "warmup_reputation": to_text((a.get("email_warmup_details") or {}).get("warmup_reputation")),
                    "is_warmup_blocked": to_text((a.get("email_warmup_details") or {}).get("is_warmup_blocked")),
                    "tag_id": tag_id,
                    "tag_name": tag_name,
                    "tag_color": tag_color,
                    "email_account_tag_mappings_count": to_text(len(tags)),
                    "email_campaign_account_mappings_count": to_text(agg.get("count")),
                }
                all_rows.append(row)
            if len(accounts) < limit:
                break
            offset += limit
    return all_rows

def run_sync(bearer: str, endpoint: str = DEFAULT_ENDPOINT, limit: int = DEFAULT_LIMIT) -> tuple[int, str]:
    with pool.connection() as conn, conn.transaction(), conn.cursor() as cur:
        cur.execute("insert into public.sync_runs (ok, message) values (null, 'started') returning id")
        sync_id = cur.fetchone()[0]
    try:
        rows = fetch_all_accounts(bearer, endpoint, limit)
        n = upsert_accounts(rows)
        with pool.connection() as conn, conn.transaction(), conn.cursor() as cur:
            cur.execute("""
                update public.sync_runs
                set finished_at = now(), ok = true, message = %s, rows_upserted = %s
                where id = %s
            """, (f"ok: upserted {n}", n, sync_id))
        return n, "ok"
    except Exception as e:
        with pool.connection() as conn, conn.transaction(), conn.cursor() as cur:
            cur.execute("""
                update public.sync_runs
                set finished_at = now(), ok = false, message = %s
                where id = %s
            """, (str(e), sync_id))
        raise

st.set_page_config(page_title="Smartlead Accounts Sync", layout="wide")
st.title("Smartlead Accounts Sync → all_accounts_realtime")

with st.sidebar:
    st.subheader("Connection")
    st.text_input("Supabase DB URL", value=SUPABASE_DB_URL, type="password", disabled=True)

    st.subheader("Smartlead API")
    existing = resolve_bearer()
    masked = f"{existing[:6]}...{existing[-4:]}" if existing and len(existing) > 10 else ("not set" if not existing else "set")
    st.write(f"Current Bearer: **{masked}**")

    new_token = st.text_input("New Bearer token", type="password", placeholder="Paste new Bearer token")
    c1, c2 = st.columns(2)
    with c1:
        if st.button("Replace Bearer"):
            if not (new_token or "").strip():
                st.error("Token cannot be empty")
            else:
                db_set_bearer(new_token.strip())
                st.session_state["bearer"] = new_token.strip()
                st.success("Bearer replaced")
                time.sleep(0.3)
                st.rerun()
    with c2:
        if st.button("Clear Bearer"):
            db_clear_bearer()
            st.session_state.pop("bearer", None)
            st.success("Bearer cleared")
            time.sleep(0.3)
            st.rerun()

    st.divider()
    st.subheader("Endpoint and paging")
    endpoint = st.text_input("Endpoint URL", value=DEFAULT_ENDPOINT)
    limit = st.number_input("Page size", min_value=100, max_value=10000, value=DEFAULT_LIMIT, step=100)

    st.divider()
    st.subheader("Run sync")
    run_now = st.button("Sync now")

tabs = st.tabs(["Status", "Preview", "Logs", "Automation"])

with tabs[0]:
    with pool.connection() as conn, conn.cursor() as cur:
        cur.execute(f"select count(*) from {TABLE_NAME}")
        total = cur.fetchone()[0]
    st.metric("Rows in all_accounts_realtime", total)

    with pool.connection() as conn, conn.cursor() as cur:
        cur.execute("select started_at, finished_at, ok, message, rows_upserted from public.sync_runs order by id desc limit 1")
        last = cur.fetchone()
    if last:
        st.write(f"Last run: started **{last[0]}**, finished **{last[1]}**, ok **{last[2]}**, message **{last[3]}**, rows **{last[4]}**")
    else:
        st.write("No runs yet")

    if run_now:
        bearer = resolve_bearer()
        if not bearer:
            st.error("Set the Bearer token first")
        else:
            with st.spinner("Syncing..."):
                n, _ = run_sync(bearer, endpoint, int(limit))
            st.success(f"Done. Upserted {n} accounts.")

with tabs[1]:
    with pool.connection() as conn, conn.cursor() as cur:
        cur.execute(f"""
            select id, from_email, from_name, type, is_smtp_success, is_imap_success,
                   message_per_day, daily_sent_count, smart_sender_flag, warmup_status,
                   warmup_reputation, is_warmup_blocked, tag_name, email_campaign_account_mappings_count
            from {TABLE_NAME}
            order by id desc limit 200
        """)
        rows = cur.fetchall()
        cols = [d.name for d in cur.description]
    st.dataframe(pd.DataFrame(rows, columns=cols), use_container_width=True)

with tabs[2]:
    with pool.connection() as conn, conn.cursor() as cur:
        cur.execute("select id, started_at, finished_at, ok, rows_upserted, message from public.sync_runs order by id desc limit 50")
        rows = cur.fetchall()
        cols = [d.name for d in cur.description]
    st.dataframe(pd.DataFrame(rows, columns=cols), use_container_width=True)

with tabs[3]:
    st.markdown("Use GitHub Actions or Supabase Cron. Cron `0 */8 * * *`.")
