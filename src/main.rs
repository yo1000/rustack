extern crate rustack;
#[macro_use]
extern crate tera;

use actix_web::{
    App,
    HttpServer,
    middleware,
};

use rustack::{
    datasource,
    envvar,
    handler,
};

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
            .data(handler::AppConfig {
                datasource,
                tera,
            })
            .wrap(middleware::Logger::default())
            .service(handler::get_tables)
    })
    .bind("127.0.0.1:8088")
    .unwrap()
    .run()
    .unwrap();
}
