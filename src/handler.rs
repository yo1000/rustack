use actix_web::{
    error,
    Error,
    get,
    HttpResponse,
    post,
    web
};

use sql::TableOutlineResult;

use crate::{
    datasource,
    sql,
};

pub struct AppConfig {
    pub datasource: datasource::DataSource,
    pub tera: tera::Tera,
}

#[derive(Serialize, Deserialize)]
struct TableOutline {
    fqn: String,
    name: String,
    column_count: u32,
    row_count: u64,
    parent_count: u32,
    child_count: u32,
    comment: String,
    note: String
}

#[get("/tables")]
pub fn get_tables(
    config: web::Data<AppConfig>,
) -> Result<HttpResponse, Error> {
    let pool = &config.datasource.conn_pool.clone();
    let conn = pool.get().unwrap();
    let db_name = &config.datasource.name;

    let table_results: Vec<TableOutlineResult> = sql::query_table_outline(
        conn, String::from(db_name));

    let tables: Vec<TableOutline> = table_results.into_iter().map(|t| {
        TableOutline {
            fqn: format!("{}.{}", db_name, t.table_name),
            name: t.table_name,
            column_count: 100,
            row_count: 200,
            parent_count: 300,
            child_count: 400,
            comment: t.table_comment.unwrap_or(String::from("")),
            note: String::from("NOTEnote")
        }
    }).collect();

    let mut ctx = tera::Context::new();
    ctx.insert("tables", &tables);

    let s = config.tera.render("tables.html", &ctx)
        .map_err(|_| error::ErrorInternalServerError("Template error"))?;

    Ok(HttpResponse::Ok()
        .content_type("text/html")
        .body(s)
    )
}
