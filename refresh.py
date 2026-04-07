#!/usr/bin/env python3
"""
Large Orders Dashboard — Data Refresh Script
Queries BigQuery, transforms data, and updates index.html in-place.
Run from the dashboard-deploy directory.
"""
import json, sys, os, subprocess
from datetime import datetime, timezone, timedelta

NOW = datetime.now(timezone.utc)

SHIP_BUFFER = {'AIR': 10, 'SEA_AIR': 15, 'SEA': 45}
SHIP_LABELS = {'AIR': 'Air', 'SEA_AIR': 'Sea-Air', 'SEA': 'Sea'}

STATUS_TS_MAP = {
    'CREATED': 6, 'ACCEPTED': 14, 'PICKUP_READY': 15,
    'PICKUP_SUCCESSFULL': 16, 'PICKUP_FAILED': 17,
    'QC_PENDING': 18, 'QC_APPROVED': 19, 'QC_HOLD': 20,
    'QC_REJECTED': 21, 'HANDED_OVER_TO_LOGISTICS_PARTNER': 22,
    'FREIGHT': 23, 'FREIGHT_DEPARTED': 24, 'FREIGHT_CUSTOMS': 25,
    'COURIER': 26, 'COURIER_CUSTOMS': 27,
}

EXCLUDE_FLEEK_IDS = {
    '31792_90','32716_30','33010_14','36028_26','36130_54','36165_66','36215_94',
    '36223_86','36249_46','36251_14','36260_22','36271_06','36271_38','36358_90',
    '36359_38','36369_42','36403_22','36403_54','36403_58','36403_90','36410_18',
    '36410_82','36440_34','36468_98','40659_26','40660_02','40790_26','43090_10',
    '43090_78','44695_18','45288_78','45924_50','46482_22','46511_58','51271_78',
    '55833_26'
}

PROJECT_ID = "dogwood-baton-345622"

# ── helpers ──

def parse_dt(val):
    if not val: return None
    s = str(val)
    try:
        f = float(s)
        if f > 1e9: return datetime.fromtimestamp(f, tz=timezone.utc)
    except: pass
    try:
        dt = datetime.fromisoformat(s.replace('Z', '+00:00'))
        if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except: pass
    try: return datetime.strptime(s[:10], '%Y-%m-%d').replace(tzinfo=timezone.utc)
    except: return None

def fmt_date(val):
    dt = parse_dt(val) if not isinstance(val, datetime) else val
    return dt.strftime('%d %b %Y') if dt else None

def fmt_month(val):
    dt = parse_dt(val) if not isinstance(val, datetime) else val
    return dt.strftime('%b %Y') if dt else None

def to_iso(val):
    dt = parse_dt(val) if not isinstance(val, datetime) else val
    return dt.isoformat() if dt else None

def calc_age(val):
    dt = parse_dt(val)
    return (NOW - dt).days if dt else 0

def v(row, idx):
    return row['f'][idx]['v']

def vf(row, idx):
    val = v(row, idx)
    if val is None or val == '': return None
    return round(float(val), 2)

def vi(row, idx):
    val = v(row, idx)
    if val is None or val == '': return None
    return int(float(val))

def run_bq(sql):
    """Run a BigQuery query via bq CLI and return parsed JSON result."""
    cmd = [
        'bq', 'query', '--project_id', PROJECT_ID,
        '--format=json', '--nouse_legacy_sql', '--max_rows=100000',
        sql
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    if result.returncode != 0:
        print(f"BQ ERROR: {result.stderr}", file=sys.stderr)
        sys.exit(1)
    return json.loads(result.stdout)

# ── main ──

def main():
    print(f"[{NOW.strftime('%H:%M:%S')}] Starting refresh...")

    # 1) Query orders
    print("Querying orders...")
    orders_sql = """
    WITH oms AS (
      SELECT order_line_id, shipping_mode, targetted_delivery_date,
        oms_status,
        ROW_NUMBER() OVER (PARTITION BY order_line_id ORDER BY oms_updated_at DESC) AS rn
      FROM `dogwood-baton-345622.sherry.oms_data_parse`
    ),
    am AS (
      SELECT customer_id_key, owner_2_ AS account_manager
      FROM `dogwood-baton-345622.google_sheets.account_ownership_data`
    )
    SELECT
      CAST(o.order_line_id AS STRING) AS order_line_id,
      o.fleek_id,
      CAST(o.order_number AS STRING) AS order_number,
      CAST(o.order_id AS STRING) AS order_id,
      CAST(o.product_id AS STRING) AS product_id,
      CAST(o.customer_id AS STRING) AS customer_id,
      CAST(o.created_at AS STRING) AS created_at,
      o.total_order_line_amount,
      ROUND(o.total_order_line_amount * 1.3, 2) AS gmv,
      o.item_name, o.product_type, o.latest_status,
      CAST(o.latest_status_date AS STRING) AS latest_status_date,
      o.replacement_status,
      CAST(o.accepted_at AS STRING) AS accepted_at,
      CAST(o.pickup_ready_at AS STRING) AS pickup_ready_at,
      CAST(o.pickup_successful_at AS STRING) AS pickup_successful_at,
      CAST(o.pickup_failed_at AS STRING) AS pickup_failed_at,
      CAST(o.qc_pending_at AS STRING) AS qc_pending_at,
      CAST(o.qc_approved_at AS STRING) AS qc_approved_at,
      CAST(o.qc_hold_at AS STRING) AS qc_hold_at,
      CAST(o.qc_rejected_at AS STRING) AS qc_rejected_at,
      CAST(o.logistics_partner_handedover_at AS STRING) AS logistics_partner_handedover_at,
      CAST(o.freight_at AS STRING) AS freight_at,
      CAST(o.freight_departed_at AS STRING) AS freight_departed_at,
      CAST(o.freight_customs_at AS STRING) AS freight_customs_at,
      CAST(o.courier_at AS STRING) AS courier_at,
      CAST(o.courier_customs_at AS STRING) AS courier_customs_at,
      CAST(o.delivered_at AS STRING) AS delivered_at,
      o.customer_name, o.customer_email, o.customer_country, o.customer_country_region,
      o.vendor, o.vendor_country, o.vendor_country_region, o.vendor_zone,
      o.courier_company, o.tracking_number, o.shipping_method,
      o.flight_number, o.logistics_partner_name, o.boxes AS flight_boxes,
      CASE
        WHEN o.vendor_country_region='PK' AND o.vendor_zone='Karachi Export Processing Zone' THEN 'PK - Zone'
        WHEN o.vendor_country_region='PK' AND o.vendor_zone IS NULL THEN 'PK - Non Zone'
        WHEN o.vendor_country_region='IN' THEN 'IN' ELSE 'ROW'
      END AS zone_mapping,
      am.account_manager,
      CASE
        WHEN am.account_manager IS NULL OR am.account_manager='' OR am.account_manager='Self Serve' THEN 'Self-Serve'
        WHEN oms.oms_status IS NOT NULL THEN 'Filled' ELSE 'Pending'
      END AS oms_form_status,
      oms.shipping_mode,
      CAST(oms.targetted_delivery_date AS STRING) AS targetted_delivery_date,
      oms.oms_status
    FROM `dogwood-baton-345622.fleek_raw.order_line_status_details` o
    LEFT JOIN am ON o.customer_id = am.customer_id_key
    LEFT JOIN (SELECT * FROM oms WHERE rn=1) oms ON CAST(o.order_line_id AS STRING) = CAST(oms.order_line_id AS STRING)
    WHERE DATE(o.created_at) >= '2025-07-01'
      AND o.total_order_line_amount * 1.3 >= 2000
      AND o.vendor NOT IN ('alexprodshop','walitest','fleek-credit','container-carpool')
      AND NOT REGEXP_CONTAINS(o.item_name, '(?i)ADDITIONAL (?:PAYMENT|CHARGES)')
    ORDER BY o.created_at DESC
    """
    orders_rows = run_bq(orders_sql)
    print(f"  Got {len(orders_rows)} orders")

    # 2) Query per-line tracking from line_item_logistic
    print("Querying tracking...")
    tracking_sql = """
    SELECT
      REPLACE(internal_order_id, '/', '_') AS fleek_id,
      TO_JSON_STRING(courier_tracking_ids) AS tracking_ids_json,
      courier_tracking_id, courier_service
    FROM `dogwood-baton-345622.aurora_postgres_public.line_item_logistic`
    WHERE (_fivetran_deleted <> true OR _fivetran_deleted IS NULL)
    """
    tracking_rows = run_bq(tracking_sql)
    print(f"  Got {len(tracking_rows)} tracking rows")

    # Build tracking map
    tracking_map = {}
    for row in tracking_rows:
        fid = row.get('fleek_id')
        tids_json = row.get('tracking_ids_json')
        tns = []
        if tids_json:
            try:
                tns = json.loads(tids_json)
                if not isinstance(tns, list): tns = [str(tns)]
            except: pass
        if not tns and row.get('courier_tracking_id'):
            tns = [row['courier_tracking_id']]
        tracking_map[fid] = {'tns': tns, 'cs': row.get('courier_service')}

    # 3) Transform orders
    print("Transforming...")
    compact = []
    for row in orders_rows:
        fid = row.get('fleek_id')
        if fid in EXCLUDE_FLEEK_IDS: continue

        created_at = row.get('created_at')
        created_dt = parse_dt(created_at)
        delivered_at = row.get('delivered_at')
        delivered_dt = parse_dt(delivered_at)
        age = calc_age(created_at)
        latest_status = row.get('latest_status')
        ofs = row.get('oms_form_status')
        tdd = row.get('targetted_delivery_date')
        smode = row.get('shipping_mode')
        am = row.get('account_manager')

        # Lifecycle
        if delivered_dt: lifecycle = 'Delivered'
        elif latest_status == 'CANCELLED': lifecycle = 'Cancelled'
        else: lifecycle = 'Open'
        is_delivered = lifecycle == 'Delivered'

        # FF logic
        delivery_dt = None; delivery_src = None; ship_mode = None
        if ofs == 'Filled':
            if tdd: delivery_dt = parse_dt(tdd); delivery_src = 'OMS'
            elif created_dt: delivery_dt = created_dt + timedelta(days=21); delivery_src = 'OMS (no date)'
            ship_mode = smode or 'AIR'
        elif ofs == 'Self-Serve':
            if created_dt: delivery_dt = created_dt + timedelta(days=21); delivery_src = 'Air-21d'
            ship_mode = 'AIR'
        else:
            delivery_src = 'Awaiting OMS'

        tff_dt = None; tff_src = None; buffer = None
        if delivery_dt and ship_mode:
            buffer = SHIP_BUFFER.get(ship_mode, 10)
            tff_dt = delivery_dt - timedelta(days=buffer)
            tff_src = f"{SHIP_LABELS.get(ship_mode, ship_mode)} (-{buffer}d)"

        qc_dt = parse_dt(row.get('qc_approved_at'))
        if is_delivered:
            ff_on = (qc_dt <= tff_dt) if (qc_dt and tff_dt) else (True if qc_dt else None)
            del_on = (delivered_dt <= delivery_dt) if (delivered_dt and delivery_dt) else (True if delivered_dt else None)
            dr = None; drd = None
            ot = (ff_on and del_on) if (ff_on is not None and del_on is not None) else None
        elif lifecycle == 'Cancelled':
            dr = None; drd = None; ot = None
        else:
            dr = (tff_dt - NOW).days if tff_dt else None
            drd = (delivery_dt - NOW).days if delivery_dt else None
            ot = 'awaiting' if ofs == 'Pending' else (dr >= 0 if dr is not None else None)

        # Status aging
        sa = None
        if not is_delivered:
            col = STATUS_TS_MAP.get(latest_status)
            if col is not None:
                ts_field = {6:'created_at',14:'accepted_at',15:'pickup_ready_at',16:'pickup_successful_at',
                    17:'pickup_failed_at',18:'qc_pending_at',19:'qc_approved_at',20:'qc_hold_at',
                    21:'qc_rejected_at',22:'logistics_partner_handedover_at',23:'freight_at',
                    24:'freight_departed_at',25:'freight_customs_at',26:'courier_at',27:'courier_customs_at'}.get(col)
                ts_val = row.get(ts_field) if ts_field else None
                ts_dt = parse_dt(ts_val)
                if ts_dt: sa = (NOW - ts_dt).days

        # Tracking
        lil = tracking_map.get(fid, {})
        tns = lil.get('tns', [])
        cs = lil.get('cs')
        if not tns:
            tn_raw = row.get('tracking_number')
            tns = [x.strip() for x in tn_raw.split(',')] if tn_raw else []
            cs = row.get('courier_company')

        rec = {
            'fid': fid, 'on': int(float(row['order_number'])) if row.get('order_number') else None,
            'olid': int(float(row['order_line_id'])) if row.get('order_line_id') else None,
            'oid': int(float(row['order_id'])) if row.get('order_id') else None,
            'pid': int(float(row['product_id'])) if row.get('product_id') else None,
            'cid': int(float(row['customer_id'])) if row.get('customer_id') else None,
            'cat': fmt_date(created_at), 'cat_iso': to_iso(created_at),
            'mo': fmt_month(created_at),
            'in': row.get('item_name'), 'pt': row.get('product_type'),
            'amt': float(row['total_order_line_amount']) if row.get('total_order_line_amount') else None,
            'gmv': float(row['gmv']) if row.get('gmv') else None,
            'age': age, 'st': latest_status,
            'sd': fmt_date(row.get('latest_status_date')),
            'rs': int(float(row['replacement_status'])) if row.get('replacement_status') else 0,
            'lc': lifecycle,
            't_acc': to_iso(row.get('accepted_at')),
            't_pr': to_iso(row.get('pickup_ready_at')),
            't_ps': to_iso(row.get('pickup_successful_at')),
            't_pf': to_iso(row.get('pickup_failed_at')),
            't_qp': to_iso(row.get('qc_pending_at')),
            't_qa': to_iso(row.get('qc_approved_at')),
            't_qh': to_iso(row.get('qc_hold_at')),
            't_qr': to_iso(row.get('qc_rejected_at')),
            't_lp': to_iso(row.get('logistics_partner_handedover_at')),
            't_fr': to_iso(row.get('freight_at')),
            't_fd': to_iso(row.get('freight_departed_at')),
            't_fc': to_iso(row.get('freight_customs_at')),
            't_co': to_iso(row.get('courier_at')),
            't_cc': to_iso(row.get('courier_customs_at')),
            't_del': to_iso(delivered_at),
            'cn': row.get('customer_name'), 'ce': row.get('customer_email'),
            'cc': row.get('customer_country'), 'ccr': row.get('customer_country_region'),
            'vn': row.get('vendor'), 'vc': row.get('vendor_country'),
            'vcr': row.get('vendor_country_region'), 'vz': row.get('vendor_zone'),
            'zm': row.get('zone_mapping'),
            'tns': tns, 'tcs': [cs]*len(tns) if cs and tns else [],
            'tn': ', '.join(tns) if tns else None,
            'cs': cs,
            'sm': row.get('shipping_method'),
            'fn': row.get('flight_number'), 'lp': row.get('logistics_partner_name'),
            'fb': row.get('flight_boxes'),
            'am': am, 'omss': row.get('oms_status'), 'ofs': ofs,
            'shm': SHIP_LABELS.get(ship_mode, ship_mode) if ship_mode else None,
            'shm_buf': buffer,
            'dd': fmt_date(delivery_dt), 'dd_iso': to_iso(delivery_dt), 'dd_src': delivery_src,
            'efd': fmt_date(tff_dt), 'efd_iso': to_iso(tff_dt), 'efd_src': tff_src,
            'dr': dr, 'drd': drd, 'ot': ot, 'sa': sa,
        }
        compact.append(rec)

    compact.sort(key=lambda x: x.get('cat_iso') or '', reverse=True)

    open_c = sum(1 for r in compact if r['lc'] == 'Open')
    del_c = sum(1 for r in compact if r['lc'] == 'Delivered')
    can_c = sum(1 for r in compact if r['lc'] == 'Cancelled')
    print(f"  Total: {len(compact)} | Open: {open_c}, Delivered: {del_c}, Cancelled: {can_c}")

    # 4) Embed into HTML
    print("Updating index.html...")
    data_json = json.dumps(compact, separators=(',', ':'))

    with open('index.html') as f:
        html = f.read()

    start = html.find('const D=')
    end = html.find(';const CD=', start)
    if end == -1: end = html.find(';', start + 8)

    html = html[:start] + f'const D={data_json}' + html[end:]

    with open('index.html', 'w') as f:
        f.write(html)

    print(f"  HTML updated: {len(html):,} chars")

    # 5) Git commit & push
    print("Pushing to GitHub...")
    subprocess.run(['git', 'add', 'index.html'], check=True)
    msg = f"auto-refresh: {len(compact)} orders ({open_c} open, {del_c} delivered, {can_c} cancelled) — {NOW.strftime('%d %b %Y %H:%M')} UTC"
    subprocess.run(['git', 'commit', '-m', msg], check=True)
    subprocess.run(['git', 'push', 'origin', 'main'], check=True)
    subprocess.run(['git', 'push', 'origin', 'gh-pages'], check=True, capture_output=True)

    print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] Done!")

if __name__ == '__main__':
    main()
