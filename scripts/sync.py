
import os, sys, httpx
from psycopg_pool import ConnectionPool

ENDPOINT = os.environ.get(
    "SMARTLEAD_ENDPOINT",
    "https://server.smartlead.ai/api/email-account/get-total-email-accounts"
)
LIMIT = int(os.environ.get("SMARTLEAD_LIMIT", "10000"))
SUPABASE_DB_URL = os.environ["SUPABASE_DB_URL"]
BEARER = os.environ["SMARTLEAD_BEARER"]
TIMEOUT = int(os.environ.get("SMARTLEAD_TIMEOUT", "60"))
TABLE_NAME = "public.all_accounts_realtime"

pool = ConnectionPool(SUPABASE_DB_URL, min_size=1, max_size=4, timeout=10)

INIT_SQL = """
create table if not exists public.sync_runs (
  id bigserial primary key,
  started_at timestamptz default now(),
  finished_at timestamptz,
  ok boolean,
  message text,
  rows_upserted integer
);

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
);
"""

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

def to_text(v):
    return None if v is None else str(v)

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

def init_db():
    with pool.connection() as conn, conn.transaction(), conn.cursor() as cur:
        cur.execute(INIT_SQL)

def log_start():
    with pool.connection() as conn, conn.transaction(), conn.cursor() as cur:
        cur.execute("insert into public.sync_runs (ok, message) values (null, 'started') returning id")
        return cur.fetchone()[0]

def log_finish(sync_id, ok, msg, rows):
    with pool.connection() as conn, conn.transaction(), conn.cursor() as cur:
        cur.execute("""
            update public.sync_runs
            set finished_at = now(), ok = %s, message = %s, rows_upserted = %s
            where id = %s
        """, (ok, msg, rows, sync_id))

def fetch_all_accounts(bearer, endpoint, limit):
    headers = {"Accept": "application/json", "Authorization": f"Bearer {bearer}"}
    all_rows, offset = [], 0
    with httpx.Client(timeout=TIMEOUT) as client:
        while True:
            resp = client.get(endpoint, headers=headers, params={"offset": offset, "limit": limit})
            if resp.status_code == 401:
                raise RuntimeError("401 Unauthorized. Check Bearer.")
            resp.raise_for_status()
            payload = resp.json()
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

def main():
    init_db()
    sync_id = log_start()
    try:
        rows = fetch_all_accounts(BEARER, ENDPOINT, LIMIT)
        n = 0
        if rows:
            with pool.connection() as conn, conn.transaction(), conn.cursor() as cur:
                cur.executemany(UPSERT_SQL, rows)
                n = len(rows)
        log_finish(sync_id, True, f"ok: upserted {n}", n)
        print(f"OK {n}")
    except Exception as e:
        log_finish(sync_id, False, str(e), 0)
        print(f"ERROR {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
