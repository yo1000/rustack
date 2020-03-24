use std::collections::HashMap;

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
use crate::sql::{Table, TableSizeResult};

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
    let mut conn = pool.get().unwrap();
    let db_name = &config.datasource.name;

    let table_results: Vec<TableOutlineResult> = sql::query_table_outline(
        &mut conn, String::from(db_name));
    let size_map: HashMap<String, TableSizeResult> = sql::query_table_size_map(
        &mut conn, String::from(db_name));
    let ref_parent_map: HashMap<String, u32> = sql::query_table_referencing_count_to_parent_map(
        &mut conn, String::from(db_name));
    let ref_children_map: HashMap<String, u32> = sql::query_table_referenced_count_from_children_map(
        &mut conn, String::from(db_name));

    let tables: Vec<TableOutline> = table_results.into_iter().map(|t| {
        let table_name: &str = t.table_name.as_str();
        TableOutline {
            fqn: format!("{}.{}", db_name, table_name),
            name: table_name.to_string(),
            column_count: size_map[table_name].columns,
            row_count: size_map[table_name].rows,
            parent_count: if ref_parent_map.contains_key(table_name) { ref_parent_map[table_name] } else { 0 },
            child_count: if ref_children_map.contains_key(table_name) { ref_children_map[table_name] } else { 0 },
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

#[derive(Serialize, Deserialize)]
pub struct TablePathVariable {
    table_name: String,
}

#[get("/table/{table_name}")]
pub fn get_table_by_name(
    config: web::Data<AppConfig>,
    path_var: web::Path<TablePathVariable>,
) -> Result<HttpResponse, Error> {
    let pool = &config.datasource.conn_pool.clone();
    let conn = pool.get().unwrap();
    let db_name = &config.datasource.name;
    let table_name = &path_var.table_name;

    let table_opt: Option<Table> = sql::query_table(
        conn,
        String::from(db_name),
        String::from(table_name),
    );

    let table = match table_opt {
        Some(t) => t,
        _ => {
            return Ok(HttpResponse::NotFound().body(""));
        }
    };

    let mut ctx = tera::Context::new();
    ctx.insert("table", &table);

    let s = config.tera.render("table.html", &ctx)
        .map_err(|_| error::ErrorInternalServerError("Template error"))?;

    Ok(HttpResponse::Ok()
        .content_type("text/html")
        .body(s)
    )
}
