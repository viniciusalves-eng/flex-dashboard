import json, subprocess, sys
from datetime import datetime
from google.cloud import bigquery

QUERY = """
SELECT
  FORMAT_DATE('%Y-%m', DATE(shp.SHP_DATE_HANDLING_ID)) AS mes,
  UPPER(adr_buyer.SHP_ADD_CITY_NAME) AS cidade,
  carrier.CARRIER_NAME AS transportadora,
  COUNT(DISTINCT shp.SHP_SHIPMENT_ID) AS qtd_total_pacotes,
  COUNT(DISTINCT IF(
    DATE(summary.SHP_FIRST_VISIT_DATE_TZ) > DATE(summary.PO_UB_DATETIME_TZ),
    shp.SHP_SHIPMENT_ID, NULL
  )) AS qtd_pacotes_atraso
FROM `meli-bi-data.WHOWNER.BT_SHP_SHIPMENTS` shp
JOIN `meli-bi-data.WHOWNER.LK_SHP_ADDRESS` adr_buyer
  ON shp.SHP_RECEIVER_ADDRESS = adr_buyer.SHP_ADD_ID
LEFT JOIN `meli-bi-data.WHOWNER.LK_SHP_FLEX_TRANSPORTATION` carrier
  ON shp.SHP_SHIPMENT_ID = carrier.SHP_SHIPMENT_ID
LEFT JOIN `meli-bi-data.WHOWNER.BT_SHP_SHIPMENTS_SUMMARY` summary
  ON shp.SHP_SHIPMENT_ID = summary.SHP_SHIPMENT_ID
WHERE
  DATE(shp.SHP_DATE_HANDLING_ID) BETWEEN
    DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH), MONTH)
    AND CURRENT_DATE()
  AND shp.SIT_SITE_ID = 'MLB'
  AND shp.shp_shipping_mode_id = 'me2'
  AND shp.shp_type = 'forward'
  AND shp.shp_source_id = 'MELI'
  AND shp.shp_status_id NOT IN ('cancelled', 'pending')
  AND UPPER(shp.SHP_PICKING_TYPE_ID) = 'SELF_SERVICE'
  AND REGEXP_CONTAINS(
    UPPER(NORMALIZE(adr_buyer.SHP_ADD_CITY_NAME, NFD)),
    r'(FRANCA|JUNDIAI|CAMPINAS|IBITINGA|SOROCABA|RIBEIRAO[[:space:]]*PRETO|EXTREMA|SAO[[:space:]]*JOSE[[:space:]]*DO[[:space:]]*RIO[[:space:]]*PRETO|CAJAMAR|BARUERI|GOIANIA|COTIA|AMERICANA|MOGI[[:space:]]*DAS[[:space:]]*CRUZES|MAUA|SAO[[:space:]]*JOSE[[:space:]]*DOS[[:space:]]*CAMPOS|LIMEIRA|SANTANA[[:space:]]*DE[[:space:]]*PARNAIBA|MARINGA|BIRIGUI|JOINVILLE|JANDIRA|SANTOS|ATIBAIA|INDAIATUBA)'
  )
GROUP BY mes, cidade, transportadora
ORDER BY mes, qtd_total_pacotes DESC
"""

def main():
    print(f"[{datetime.now():%Y-%m-%d %H:%M}] Iniciando query no BigQuery...")
    client = bigquery.Client(project="meli-bi-data")
    rows = list(client.query(QUERY).result())

    if not rows:
        print("ERRO: query retornou 0 linhas.")
        sys.exit(1)

    data = [
        {
            "mes": r.mes,
            "cidade": r.cidade,
            "transportadora": r.transportadora or "",
            "qtd_total_pacotes": int(r.qtd_total_pacotes or 0),
            "qtd_pacotes_atraso": int(r.qtd_pacotes_atraso or 0),
        }
        for r in rows
    ]

    output = {
        "atualizado_em": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "total_registros": len(data),
        "dados": data,
    }

    with open("data.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"[OK] {len(data)} registros salvos em data.json")

    subprocess.run(["git", "add", "data.json"], check=True)
    subprocess.run(["git", "commit", "-m", f"data: atualiza volume flex {datetime.now():%Y-%m-%d}"], check=True)
    subprocess.run(["git", "push"], check=True)
    print("[OK] Push realizado — dashboard atualizado.")

if __name__ == "__main__":
    main()
