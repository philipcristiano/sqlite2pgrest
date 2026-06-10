use clap::Parser;
use serde::{Deserialize, Serialize};

use sqlx::sqlite::SqlitePool;


#[derive(Parser, Debug)]
pub struct Args {
    #[arg(short, long, default_value = "127.0.0.1:3002")]
    bind_addr: String,
    #[arg(short, long, default_value = "cma.toml")]
    config_file: String,
    #[arg(short, long)]
    sqlite_file: String,
    #[arg(short, long)]
    postgrest_url: String,
    #[arg(long, default_value = "public")]
    postgrest_schema: String,
    #[arg(long)]
    token: Option<String>,
    #[arg(long)]
    token_path: Option<String>,
    #[arg(short, long, value_enum, default_value = "DEBUG")]
    log_level: tracing::Level,
    #[arg(long, action)]
    log_json: bool,
    #[arg(short, long, action=clap::ArgAction::Append)]
    table: Vec<String>,
}

#[derive(Clone, Debug, Deserialize)]
struct AppConfig {
    database_url: String,
}

#[derive(Clone, Debug)]
struct AppState {
    db: SqlitePool,
}

impl AppState {
    fn from_config(item: AppConfig, db: SqlitePool) -> Self {
        AppState { db }
    }
}

fn read_app_config(path: String) -> AppConfig {
    use std::fs;
    let config_file_error_msg = format!("Could not read config file {}", path);
    let config_file_contents = fs::read_to_string(path).expect(&config_file_error_msg);
    let app_config: AppConfig =
        toml::from_str(&config_file_contents).expect("Problems parsing config file");

    app_config
}

use futures_util::StreamExt;
use sqlx::sqlite::SqliteRow;
use sqlx::{Row, Column};
use sqlx::TypeInfo;
#[tokio::main]
async fn main() -> anyhow::Result<()>{
    let args = Args::parse();
    service_conventions::tracing::setup(args.log_level);

    let app_config = read_app_config(args.config_file);
    let token = if let Some(token_path) = args.token_path {
        Some(std::fs::read_to_string(token_path)?)
    } else {
        args.token.clone()
    };

    // start by making a database connection.
    tracing::info!("connecting to database");
    let pool = SqlitePool::connect(&app_config.database_url)
        .await
        .expect("cannot connect to db");
    tracing::info!("connecting to calibre-web database");
    for table in args.table {
        tracing::info!(table=?table, "tables");
        let url = format!("{}/{}", args.postgrest_url, table.clone());
        let mut qb: sqlx::QueryBuilder<sqlx::Sqlite> = sqlx::QueryBuilder::new("SELECT * FROM ");
        qb.push(table);
        let mut rows = sqlx::query(qb.sql()).fetch(&pool);
        let mut buffer = vec!();

        while let Some(Ok(row)) = rows.next().await {
            let new = row_to_map(&row);
            buffer.push(new);
            if buffer.len() > 100 {
                tracing::info!("Sending batch");
                send_rows(&url, &args.postgrest_schema, &token, &buffer).await?;
                tracing::info!("Sent batch");
                buffer.clear();
            }
        }
        if !buffer.is_empty() {
            tracing::info!("Sending batch");
            send_rows(&url, &args.postgrest_schema, &token, &buffer).await?;
            tracing::info!("Sent batch");
        }
    }
    Ok(())

}
use std::collections::HashMap;
fn row_to_map(row: &SqliteRow) -> HashMap<String, serde_json::Value> {
    let mut map = HashMap::new();
    for col in row.columns() {
        let name = col.name();
        let val = match col.type_info().name().to_uppercase().as_str() {
            "INTEGER" => row.try_get::<i64, _>(name)
                .map(serde_json::Value::from)
                .unwrap_or(serde_json::Value::Null),
            "REAL" => row.try_get::<f64, _>(name)
                .map(serde_json::Value::from)
                .unwrap_or(serde_json::Value::Null),
            "BOOLEAN" => row.try_get::<bool, _>(name)
                .map(serde_json::Value::from)
                .unwrap_or(serde_json::Value::Null),
            _ => row.try_get::<String, _>(name)
                .map(serde_json::Value::from)
                .unwrap_or(serde_json::Value::Null),
        };
        map.insert(name.to_string(), val);
    }
    map
}

async fn send_rows(url: &String, schema: &String, token: &Option<String>, rows: &Vec<HashMap<String, serde_json::Value>>) -> anyhow::Result<()> {
    let mut client = reqwest::Client::new();
    let mut req = client.request(http::Method::POST, url).
        header("Prefer", "resolution=merge-duplicates").
        header("Content-Profile", schema);
    tracing::info!(url=url, method="post", content_profile=?schema, "Request");
    if let Some(t) = token {
        let bearer = format!("Bearer {t}");
        tracing::info!("Bearer token set");
        req = req.header("Authorization", bearer);
    }
    let resp = req.json(&rows).send().await?;
    tracing::info!(response=?resp, "Response");
    resp.error_for_status()?;
    Ok(())

}

// async fn get_authors(State(app_state): State<AppState>) -> Result<Response, AppError> {
//     let recs = sqlx::query_as!(
//         Author,
//         r#"
//             SELECT id, name, sort, link
//             FROM authors
//         "#
//     )
//     .fetch_all(&app_state.db)
//     .await?;
//
//     let cdbstruct = recs.into_iter().map(CDBStruct::Author).collect();
//     let resp = V1APIResponse { data: cdbstruct };
//     Ok(Json(resp).into_response())
// }

// Make our own error that wraps `anyhow::Error`.
#[derive(Debug)]
struct AppError(anyhow::Error);

