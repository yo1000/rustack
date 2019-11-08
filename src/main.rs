extern crate rustack;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate tera;

use actix_web::{
    App,
    error,
    Error,
    get,
    HttpResponse,
    HttpServer,
    middleware,
    post,
    web
};

use rustack::{
    datasource,
    envvar,
    sql,
};
use rustack::sql::TableOutlineResult;

#[get("/tables")]
fn get_tables(
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

fn main() {
    std::env::set_var("RUST_LOG", "actix_web=info");
    env_logger::init();

    HttpServer::new(|| {
        let host = envvar::load::<String>(datasource::DATABASE_HOST, Some(String::from(datasource::DATABASE_HOST_DEFAULT)));
        let port = envvar::load::<u32>(datasource::DATABASE_PORT, Some(datasource::DATABASE_PORT_DEFAULT));
        let username = envvar::load::<String>(datasource::DATABASE_USERNAME, None);
        let password = envvar::load::<String>(datasource::DATABASE_PASSWORD, None);
        let name = envvar::load::<String>(datasource::DATABASE_NAME, None);
        let pool_size = envvar::load::<u32>(datasource::DATABASE_POOL_SIZE, Some(datasource::DATABASE_POOL_SIZE_DEFAULT));

        let datasource = datasource::DataSource::new(
            host, port, username, password, name, pool_size
        );

        let tera = compile_templates!(
            concat!(env!("CARGO_MANIFEST_DIR"),
            "/templates/**/*"));

        App::new()
            .data(AppConfig {
                datasource,
                tera,
            })
            .wrap(middleware::Logger::default())
            .service(get_tables)
    })
    .bind("127.0.0.1:8088")
    .unwrap()
    .run()
    .unwrap();
}

pub struct AppConfig {
    pub datasource: datasource::DataSource,
    pub tera: tera::Tera,
}
