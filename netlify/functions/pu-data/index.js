const { AthenaClient, StartQueryExecutionCommand, GetQueryExecutionCommand, GetQueryResultsCommand } = require("@aws-sdk/client-athena");

const REGION = "us-east-1";
const DATABASE = "flockmail";
const S3_OUTPUT = "s3://aws-athena-query-results-852913536220-us-east-1/wg_primary/";

const PARTNERS = [
  { name: "Hostinger",       filter: "partner_name = 'Hostinger'",                                                                                                    mbx: null,                 color: "#4f46e5" },
  { name: "Bluehost",        filter: "partner_name = 'Bluehost'",                                                                                                     mbx: "mbx_created_ever=1", color: "#0891b2" },
  { name: "HostGator US",    filter: "partner_name = 'HostGator US'",                                                                                                 mbx: "mbx_created_ever=1", color: "#d97706" },
  { name: "Namesilo",        filter: "partner_name = 'Namesilo'",                                                                                                     mbx: null,                 color: "#16a34a" },
  { name: "Name.com",        filter: "partner_name = 'Name.com'",                                                                                                     mbx: null,                 color: "#dc2626" },
  { name: "Crazy Domains",   filter: "lower(trim(partner_name)) = 'crazy domains'",                                                                                   mbx: null,                 color: "#7c3aed" },
  { name: "HostGator LatAm", filter: "partner_name in ('HostGator LatAm','Hostgator Brazil','Hostgator Mexico','Hostgator Colombia','Hostgator Chile')",               mbx: null,                 color: "#db2777" },
];

function makeQuery(partnerFilter, mbxFilter) {
  const mbxCond = mbxFilter ? ` and ${mbxFilter}` : "";
  return `
with upfront as (
    select date_trunc('month', first_txn_date) txn_month,
           count(distinct order_id) domains,
           count(distinct case when first_txn_plan_type in ('premium','ultra') then order_id end) prem_ultra_domains
    from flockmail.domain_aggregate_metrics
    where first_txn_date >= date('2023-01-01')
      and ${partnerFilter}
      and lower(clean_source) != 'migration'${mbxCond}
    group by 1
),
upgrades as (
    select date_trunc('month', least(
             coalesce(premium_conversion_date, date_add('day',40,current_date)),
             coalesce(date(first_ultra_ts),    date_add('day',40,current_date))
           )) txn_month,
           count(distinct order_id) domains
    from flockmail.domain_aggregate_metrics
    where first_txn_date >= date('2023-01-01')
      and first_txn_plan_type not in ('premium','ultra')
      and ${partnerFilter}
      and lower(clean_source) != 'migration'${mbxCond}
    group by 1
),
base as (
    select coalesce(a.txn_month, b.txn_month) txn_month,
           coalesce(a.domains,0) + coalesce(b.domains,0) domains,
           coalesce(a.prem_ultra_domains,0) + coalesce(b.domains,0) total_prem_ultra_domains
    from upfront a full join upgrades b on a.txn_month = b.txn_month
    where coalesce(a.txn_month, b.txn_month) <= date_trunc('month', current_date)
      and coalesce(a.txn_month, b.txn_month) >= date_add('month', -6, date_trunc('month', current_date))
)
select date(txn_month) txn_month,
       round(total_prem_ultra_domains * 100.0 / nullif(domains, 0), 2) pu_pct
from base order by 1`;
}

async function runQuery(client, sql) {
  const start = await client.send(new StartQueryExecutionCommand({
    QueryString: sql,
    QueryExecutionContext: { Database: DATABASE },
    ResultConfiguration: { OutputLocation: S3_OUTPUT },
  }));
  const qid = start.QueryExecutionId;

  // Poll until done
  while (true) {
    const status = await client.send(new GetQueryExecutionCommand({ QueryExecutionId: qid }));
    const state = status.QueryExecution.Status.State;
    if (state === "SUCCEEDED") break;
    if (state === "FAILED" || state === "CANCELLED") throw new Error(`Query ${qid} ended: ${state}`);
    await new Promise(r => setTimeout(r, 800));
  }

  const results = await client.send(new GetQueryResultsCommand({ QueryExecutionId: qid }));
  const rows = results.ResultSet.Rows.slice(1); // skip header
  return rows.map(row => ({
    month: row.Data[0].VarCharValue,
    pct: parseFloat(row.Data[1].VarCharValue),
  }));
}

exports.handler = async (event) => {
  const CORS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type",
    "Access-Control-Allow-Methods": "GET, OPTIONS",
  };

  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 200, headers: CORS, body: "" };
  }

  try {
    const accessKeyId = process.env.TITAN_AWS_ACCESS_KEY_ID;
    const secretAccessKey = process.env.TITAN_AWS_SECRET_ACCESS_KEY;

    if (!accessKeyId || !secretAccessKey) {
      throw new Error(`Missing credentials. TITAN_AWS_ACCESS_KEY_ID=${!!accessKeyId} TITAN_AWS_SECRET_ACCESS_KEY=${!!secretAccessKey}`);
    }

    const sessionToken = process.env.TITAN_AWS_SESSION_TOKEN;
    const client = new AthenaClient({
      region: process.env.TITAN_AWS_REGION || REGION,
      credentials: {
        accessKeyId,
        secretAccessKey,
        ...(sessionToken && { sessionToken }),
      },
    });

    // Run all 7 partner queries in parallel
    const results = await Promise.all(
      PARTNERS.map(async (p) => {
        const rows = await runQuery(client, makeQuery(p.filter, p.mbx));
        return { name: p.name, color: p.color, mbx: !!p.mbx, data: rows.map(r => r.pct), rows };
      })
    );

    const labels = results[0].rows.map(r => r.month);
    const partners = results.map(r => ({ name: r.name, color: r.color, mbx: r.mbx, data: r.data }));

    return {
      statusCode: 200,
      headers: { ...CORS, "Content-Type": "application/json" },
      body: JSON.stringify({ labels, partners, updatedAt: new Date().toISOString() }),
    };
  } catch (err) {
    console.error("Function error:", err.message, err.stack);
    return {
      statusCode: 500,
      headers: CORS,
      body: JSON.stringify({ error: err.message, stack: err.stack }),
    };
  }
};
