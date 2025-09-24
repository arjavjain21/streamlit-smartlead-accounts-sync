
# Smartlead → Supabase (all_accounts_realtime) with Streamlit UI

- Writes to **public.all_accounts_realtime** with the exact schema you provided.
- Uses Supabase **transaction pooler** URL.
- Streamlit is the control panel. Scheduler runs outside Streamlit.

## Env vars
```
SUPABASE_DB_URL=postgresql://postgres.auzoezucrrhrtmaucbbg:SB0dailyreporting@aws-1-us-east-2.pooler.supabase.com:6543/postgres
SMARTLEAD_ENDPOINT=https://server.smartlead.ai/api/email-account/get-total-email-accounts
SMARTLEAD_LIMIT=10000
SMARTLEAD_TIMEOUT=60
# SMARTLEAD_BEARER is optional here, you can set it in the Streamlit UI
```

## Deploy
- Deploy `app.py` to Streamlit Cloud. Add the env vars in Secrets. Open the app and paste a fresh Bearer in the sidebar, then **Sync now**.

## Schedule every 8 hours
Use **GitHub Actions** included here or Supabase Cron. Cron expression: `0 */8 * * *`.

Schema notes:
- Tag fields are **comma-joined** if multiple tags exist.
- DNS booleans and warmup booleans are stored as **text** to match your schema.
