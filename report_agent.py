"""
Daily Executive Brief Agent
- Standalone Railway service (worker)
- Query PostgreSQL — selalu ambil data bulan/periode TERAKHIR per tabel
- Generate report via Claude API
- Kirim WhatsApp via Fonnte jam 06.00 WIB
"""

import os, time, json, requests, psycopg2, psycopg2.extras, schedule, anthropic
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL      = os.getenv("DATABASE_URL", "")
FONNTE_TOKEN      = os.getenv("FONNTE_TOKEN", "")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
WA_TARGETS        = os.getenv("REPORT_WA_TARGETS", "")
SEND_TIME_UTC     = os.getenv("REPORT_SEND_TIME_UTC", "23:00")
WIB               = timezone(timedelta(hours=7))

# ─── DB ───────────────────────────────────────────────────────────────────────
def get_conn():
    url = DATABASE_URL.replace("postgres://", "postgresql://", 1)
    return psycopg2.connect(url, sslmode="require")

def q(sql, params=None):
    try:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, params or ())
                return [dict(r) for r in cur.fetchall()]
    except Exception as e:
        print(f"  [DB WARN] {e}")
        return []

# ─── DATA GATHERING ───────────────────────────────────────────────────────────
# Strategi per tabel:
# • Punya code_current  → WHERE code_current = 1  (sudah di-flag sebagai data aktif/terbaru)
# • Punya periode (Text)→ WHERE periode = MAX(periode)
# • Punya month_update  → WHERE month_update = MAX(month_update)
# • Punya bulan+tahun   → WHERE tahun = MAX(tahun) AND bulan = MAX(bulan) pada tahun itu
# • Punya report_date   → WHERE report_date = MAX(report_date)

def gather_data():
    data = {}

    # 1. BAD ACTOR — filter periode terbaru, hanya yang belum closed
    data["bad_actor"] = q("""
        SELECT ru, tag_number, status, problem, action_plan,
               progress, target_date, periode
        FROM bad_actor_monitoring
        WHERE periode = (SELECT MAX(periode) FROM bad_actor_monitoring)
          AND LOWER(COALESCE(status,'')) NOT IN ('closed','complete','selesai','done')
        ORDER BY ru, tag_number
        LIMIT 20
    """)

    # 2. ICU — filter report_date terbaru, hanya yang belum closed
    data["icu"] = q("""
        SELECT ru, tag_no, icu_status, issue, mitigation,
               progress, target_closed, report_date
        FROM icu_monitoring
        WHERE report_date = (SELECT MAX(report_date) FROM icu_monitoring)
          AND LOWER(COALESCE(icu_status,'')) NOT IN ('closed','resolved','selesai')
        ORDER BY ru, tag_no
        LIMIT 20
    """)

    # 3. ZERO CLAMP — yang masih terpasang (belum dilepas), semua periode
    data["zero_clamp"] = q("""
        SELECT ru, area, unit, tag_no_ln, services, description,
               type_damage, tanggal_dipasang, tanggal_rencana_perbaikan,
               status, remarks
        FROM zero_clamp
        WHERE tanggal_dilepas IS NULL OR TRIM(COALESCE(tanggal_dilepas,'')) = ''
        ORDER BY tanggal_dipasang ASC NULLS LAST
        LIMIT 15
    """)

    # 4. PIPELINE — filter bulan+tahun terbaru, rem_life rendah atau ada temp repair
    data["pipeline"] = q("""
        SELECT refinery_unit, tag_number, fluida_service, nps,
               from_location, to_location, last_measured_thickness,
               rem_life_years, jumlah_temporary_repair, remarks,
               next_inspection_date, bulan, tahun
        FROM pipeline_inspection
        WHERE tahun = (SELECT MAX(tahun) FROM pipeline_inspection)
          AND bulan = (
              SELECT bulan FROM pipeline_inspection
              WHERE tahun = (SELECT MAX(tahun) FROM pipeline_inspection)
              ORDER BY bulan DESC LIMIT 1
          )
          AND (rem_life_years < 3 OR jumlah_temporary_repair > 0)
        ORDER BY rem_life_years ASC NULLS LAST
        LIMIT 15
    """)

    # 5. PAF — code_current = 1 (sudah di-flag data terbaru), hanya yang warna warning
    data["paf"] = q("""
        SELECT ru, type, target_realisasi, value, plan_unplan,
               month_update, color
        FROM paf
        WHERE code_current = 1
          AND LOWER(COALESCE(color,'')) IN ('red','yellow','orange','merah','kuning')
        ORDER BY ru, type
        LIMIT 20
    """)

    # 6. ISSUE PAF — code_current = 1
    data["issue_paf"] = q("""
        SELECT ru, type, date, issue, month_update
        FROM issue_paf
        WHERE code_current = 1
        ORDER BY date DESC NULLS LAST
        LIMIT 15
    """)

    # 7. POWER & STEAM — code_current = 1, status tidak normal
    data["power_utility"] = q("""
        SELECT refinery_unit, type_equipment, equipment,
               status_operation, status_n0, average_actual,
               desain, kapasitas_max, remark, date_update
        FROM power_stream
        WHERE code_current = 1
          AND LOWER(COALESCE(status_operation,'')) NOT IN ('normal','standby','ok','siaga')
        ORDER BY refinery_unit, type_equipment
        LIMIT 15
    """)

    # 8. CRITICAL EQUIPMENT UTL — code_current = 1, ada highlight issue
    data["critical_utl"] = q("""
        SELECT refinery_unit, type_equipment, highlight_issue,
               corrective_action, target_corrective, traffic_corrective,
               mitigasi_action, target_mitigasi, traffic_mitigasi
        FROM critical_eqp_utl
        WHERE code_current = 1
          AND TRIM(COALESCE(highlight_issue,'')) != ''
        ORDER BY refinery_unit
        LIMIT 10
    """)

    # 9. READINESS JETTY — month_update terbaru, yang ada concern
    data["readiness_jetty"] = q("""
        SELECT refinery_unit, area, unit, tag_no, status_operation,
               status_tuks, expired_tuks, status_ijin_ops, expired_ijin_ops,
               status_isps, expired_isps, status_struktur, remark_struktur,
               status_trestle, remark_trestle, status_mla, remark_mla,
               status_fire_protection, remark_fire_protection, month_update
        FROM readiness_jetty
        WHERE month_update = (SELECT MAX(month_update) FROM readiness_jetty)
          AND (
            LOWER(COALESCE(status_operation,''))  NOT IN ('normal','siap','ok','ready')
            OR LOWER(COALESCE(status_tuks,''))    NOT IN ('valid','ok','aktif')
            OR LOWER(COALESCE(status_ijin_ops,'')) NOT IN ('valid','ok','aktif')
            OR LOWER(COALESCE(status_isps,''))    NOT IN ('valid','ok','aktif')
          )
        ORDER BY refinery_unit, tag_no
        LIMIT 10
    """)

    # 10. WORKPLAN JETTY — month_update terbaru, item belum selesai
    data["workplan_jetty"] = q("""
        SELECT refinery_unit, area, unit, tag_no, item,
               status_item, remark, rtl_action_plan,
               target, keterangan, status_rtl, month_update
        FROM workplan_jetty
        WHERE month_update = (SELECT MAX(month_update) FROM workplan_jetty)
          AND LOWER(COALESCE(status_item,'')) NOT IN ('done','selesai','complete','closed')
        ORDER BY refinery_unit, tag_no
        LIMIT 15
    """)

    # 11. READINESS TANK — month_update terbaru, yang ada issue
    data["readiness_tank"] = q("""
        SELECT refinery_unit, area, unit, tag_number,
               type_tangki, service_tangki, prioritas, status_operational,
               status_coi, coi_date_expired, atg_certification_validity,
               date_expired_atg, status_atg, remark_atg,
               status_grounding, status_shell_course, remark_shell_course,
               status_roof, remark_roof, month_update
        FROM readiness_tank
        WHERE month_update = (SELECT MAX(month_update) FROM readiness_tank)
          AND (
            LOWER(COALESCE(status_operational,'')) NOT IN ('normal','ok','siap')
            OR LOWER(COALESCE(status_coi,''))      NOT IN ('valid','ok','aktif')
            OR LOWER(COALESCE(status_atg,''))      NOT IN ('ok','normal','aktif')
          )
        ORDER BY refinery_unit, tag_number
        LIMIT 15
    """)

    # 12. WORKPLAN TANK — month_update terbaru, belum selesai
    data["workplan_tank"] = q("""
        SELECT unit, tag_no, item, remark, rtl_action_plan,
               target, keterangan, status_rtl, month_update
        FROM workplan_tank
        WHERE month_update = (SELECT MAX(month_update) FROM workplan_tank)
          AND LOWER(COALESCE(status_rtl,'')) NOT IN ('done','selesai','complete','closed')
        ORDER BY tag_no
        LIMIT 15
    """)

    # 13. READINESS SPM — month_update terbaru, yang ada issue
    data["readiness_spm"] = q("""
        SELECT refinery_unit, area, unit, tag_no, status_operation,
               status_laik_operasi, expired_laik_operasi,
               status_ijin_spl, expired_ijin_spl,
               status_mbc, remark_mbc, status_lds, remark_lds,
               status_mooring_hawser, remark_mooring_hawser,
               status_floating_hose, remark_floating_hose,
               status_cathodic_spl, status_cathodic_spm, month_update
        FROM readiness_spm
        WHERE month_update = (SELECT MAX(month_update) FROM readiness_spm)
          AND (
            LOWER(COALESCE(status_operation,''))    NOT IN ('normal','siap','ok','ready')
            OR LOWER(COALESCE(status_laik_operasi,'')) NOT IN ('valid','ok','aktif')
          )
        ORDER BY refinery_unit, tag_no
        LIMIT 10
    """)

    # 14. ATG — month_update terbaru, yang ada issue
    data["atg"] = q("""
        SELECT refinery_unit, tag_no_tangki, tag_no_atg,
               status_atg, status_interkoneksi_atg,
               cert_no_atg, date_expired_atg,
               remark, rtl, status_rtl, month_update
        FROM atg_monitoring
        WHERE month_update = (SELECT MAX(month_update) FROM atg_monitoring)
          AND (
            LOWER(COALESCE(status_interkoneksi_atg,'')) NOT IN ('aktif','active','ok')
            OR LOWER(COALESCE(status_atg,''))           NOT IN ('ok','normal','aktif')
          )
        ORDER BY refinery_unit, tag_no_tangki
        LIMIT 15
    """)

    # 15. MONITORING OPERASI — code_current = 1, yang ada limitasi
    data["monitoring_operasi"] = q("""
        SELECT refinery_unit, unit_proses, unit, actual, target_sts,
               plant_readiness, limitasi_alert_process, mitigasi_process,
               limitasi_alert_sts, mitigasi_sts, month_update
        FROM monitoring_operasi
        WHERE code_current = 1
          AND (
            TRIM(COALESCE(limitasi_alert_process,'')) != ''
            OR (actual IS NOT NULL AND target_sts IS NOT NULL AND actual < target_sts)
          )
        ORDER BY refinery_unit, unit_proses
        LIMIT 15
    """)

    return data


# ─── REPORT GENERATION ────────────────────────────────────────────────────────
def generate_report(data):
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    now_wib = datetime.now(WIB)
    summary = {k: len(v) for k, v in data.items()}
    data_str = json.dumps(data, ensure_ascii=False, default=str, indent=2)

    prompt = f"""Kamu adalah sistem pelaporan otomatis Daily Executive Brief untuk operasional kilang minyak.

Tanggal: {now_wib.strftime('%A, %d %B %Y')} | Waktu: {now_wib.strftime('%H.%M')} WIB

JUMLAH ITEM PER SEKSI (hanya yang bermasalah/open):
{json.dumps(summary, indent=2)}

DATA LENGKAP DARI DATABASE:
{data_str}

INSTRUKSI:
Buat Daily Executive Brief dalam format WhatsApp. Analisa data, identifikasi asset paling kritis, sebutkan tag/asset number spesifik dari data. Maksimal 3500 karakter.

FORMAT WAJIB:
📌 DAILY EXECUTIVE BRIEF
🗓️ {now_wib.strftime('%A, %d %B %Y')} | ⏰ {now_wib.strftime('%H.%M')} WIB

🏭 RINGKASAN EKSEKUTIF
[2-3 kalimat kondisi umum berdasarkan data nyata]

━━━━━━━━━━
🔴 PRIORITAS HARI INI
━━━━━━━━━━
[3-5 isu paling kritis dengan tag number spesifik, tindak lanjut, dan PIC usulan]

━━━━━━━━━━
📍 STATUS RELIABILITY & OPERASI
━━━━━━━━━━
• Operasi: [🟢/🟡/🟠/🔴 + keterangan]
• Reliability: [status + keterangan]
• PAF/Availability: [status + keterangan]
• Power/Utility: [status + keterangan]
• ICU/Zero Clamp: [status + keterangan]

━━━━━━━━━━
⚙️ BAD ACTOR WATCHLIST
━━━━━━━━━━
[Top 5: Tag | RU | Status | Progress]

━━━━━━━━━━
🚢 READINESS ALERT
━━━━━━━━━━
• Jetty: [status + ringkasan]
• Tank: [status + ringkasan]
• SPM: [status + ringkasan]
• ATG: [status + ringkasan]

━━━━━━━━━━
🎯 TINDAK LANJUT HARI INI
━━━━━━━━━━
[4-5 action item spesifik berbasis data]

Legend: 🟢 Terkendali | 🟡 Watch | 🟠 Action | 🔴 Urgent
_Auto-generated · PRISMA Report Agent_"""

    msg = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=1500,
        messages=[{"role": "user", "content": prompt}]
    )
    return msg.content[0].text


# ─── WHATSAPP ─────────────────────────────────────────────────────────────────
def send_wa(target, message):
    try:
        resp = requests.post(
            "https://api.fonnte.com/send",
            headers={"Authorization": FONNTE_TOKEN},
            data={"target": target, "message": message},
            timeout=30
        )
        result = resp.json()
        ok = result.get("status", False)
        print(f"  [WA] {'✅' if ok else '❌'} {target} — {result}")
        return ok
    except Exception as e:
        print(f"  [WA] ❌ {target} — {e}")
        return False


# ─── JOB ──────────────────────────────────────────────────────────────────────
def run_report_job():
    now_wib = datetime.now(WIB)
    print(f"\n{'='*50}")
    print(f"[AGENT] ▶ {now_wib.strftime('%Y-%m-%d %H:%M WIB')}")
    print(f"{'='*50}")

    print("[1/3] 🔍 Ambil data (periode/bulan terakhir per tabel)...")
    data = gather_data()
    for k, v in data.items():
        print(f"  {k}: {len(v)} baris")

    print("[2/3] 🤖 Generate report (Claude)...")
    try:
        report = generate_report(data)
        print(f"  ✅ {len(report)} karakter")
    except Exception as e:
        print(f"  ❌ {e}")
        return

    targets = [t.strip() for t in WA_TARGETS.split(",") if t.strip()]
    if not targets:
        print("[3/3] ⚠️  Tidak ada target WA — preview saja:")
        print("\n" + report + "\n")
        return

    print(f"[3/3] 📤 Kirim ke {len(targets)} nomor...")
    for t in targets:
        send_wa(t, report)
        time.sleep(2)
    print("[AGENT] ✅ Done.\n")


# ─── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    print("=" * 50)
    print("  PRISMA Daily Report Agent")
    print("=" * 50)
    print(f"  Jadwal  : {SEND_TIME_UTC} UTC  =  06.00 WIB")
    print(f"  DB      : {'✅' if DATABASE_URL else '❌ Missing'}")
    print(f"  Fonnte  : {'✅' if FONNTE_TOKEN else '❌ Missing'}")
    print(f"  Claude  : {'✅' if ANTHROPIC_API_KEY else '❌ Missing'}")
    print(f"  Target  : {WA_TARGETS or '❌ Missing'}")
    print("=" * 50)

    if os.getenv("RUN_NOW", "").lower() in ("1", "true", "yes"):
        print("\n[AGENT] RUN_NOW=true — langsung jalankan...")
        run_report_job()

    schedule.every().day.at(SEND_TIME_UTC).do(run_report_job)
    print(f"\n[AGENT] ⏳ Menunggu jadwal {SEND_TIME_UTC} UTC...\n")

    while True:
        schedule.run_pending()
        time.sleep(30)


if __name__ == "__main__":
    main()
