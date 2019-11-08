extern crate r2d2;
extern crate r2d2_mysql;
extern crate mysql;

use std::{
    env,
    sync::Arc,
};

use mysql::{
    Opts,
    OptsBuilder,
};
use r2d2_mysql::MysqlConnectionManager;

use crate::envvar;

pub const DATABASE_HOST: &str = "DATABASE_HOST";
pub const DATABASE_PORT: &str = "DATABASE_PORT";
pub const DATABASE_USERNAME: &str = "DATABASE_USERNAME";
pub const DATABASE_PASSWORD: &str = "DATABASE_PASSWORD";
pub const DATABASE_NAME: &str = "DATABASE_NAME";
pub const DATABASE_POOL_SIZE: &str = "DATABASE_POOL_SIZE";

pub const DATABASE_HOST_DEFAULT: &str = "127.0.0.1";
pub const DATABASE_PORT_DEFAULT: u32 = 3306;
pub const DATABASE_POOL_SIZE_DEFAULT: u32 = 4;

pub struct DataSource {
    pub host: String,
    pub port: u32,
    pub username: String,
    password: String,
    pub name: String,
    pub conn_url: String,
    pub pool_size: u32,
    pub conn_pool: Arc<r2d2::Pool<MysqlConnectionManager>>,
}

impl DataSource {
    pub fn new(
        host: String,
        port: u32,
        username: String,
        password: String,
        name: String,
        pool_size: u32,
    ) -> DataSource {
        assert_ne!(host, "");
        assert!(port > 0);
        assert_ne!(username, "");
        assert_ne!(password, "");
        assert_ne!(name, "");
        assert!(pool_size > 0);

        let conn_url = format!(
            "mysql://{user}:{pass}@{host}:{port}/{name}",
            user = username,
            pass = password,
            host = host,
            port = port,
            name = name,
        );

        let opts = Opts::from_url(&conn_url).unwrap();
        let builder = OptsBuilder::from_opts(opts);
        let manager = MysqlConnectionManager::new(builder);
        let conn_pool = conn_pool(&conn_url, pool_size);

        let data_source = DataSource {
            host,
            port,
            username,
            password,
            name,
            pool_size,
            conn_url,
            conn_pool,
        };

        data_source
    }
}

fn conn_pool(conn_url: &String, pool_size: u32) -> Arc<r2d2::Pool<MysqlConnectionManager>> {
    let opts = Opts::from_url(&conn_url).unwrap();
    let builder = OptsBuilder::from_opts(opts);

    let manager = MysqlConnectionManager::new(builder);

    Arc::new(r2d2::Pool::builder()
        .max_size(pool_size)
        .build(manager).unwrap())
}
