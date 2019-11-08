extern crate mysql;
extern crate r2d2;
extern crate r2d2_mysql;

use std::env;
use std::sync::Arc;

use mysql::{
    Opts,
    OptsBuilder,
};
use r2d2_mysql::MysqlConnectionManager;

use self::r2d2::PooledConnection;


pub fn query_table_outline(
    mut conn: PooledConnection<MysqlConnectionManager>,
    param: String
) -> Vec<TableOutlineResult> {
    return conn.prep_exec(r#"
            SELECT
                tbl.table_name      AS table_name,
                tbl.table_comment   AS table_comment,
                CONCAT(tbl.table_schema, '.', tbl.table_name)
                                    AS table_fqn
            FROM
                information_schema.tables tbl
            WHERE
                tbl.table_schema = :param_schema_name
            AND tbl.table_type = 'BASE TABLE'
            ORDER BY
                tbl.table_name
            "#, params!{
                "param_schema_name" => param
            })
        .map::<Vec<TableOutlineResult>, _>(|result| {
            result
                .map(|x| x.unwrap())
                .map(|row| {
                    let (table_name, table_comment, table_fqn) = mysql::from_row(row);
                    TableOutlineResult {
                        table_name,
                        table_comment,
                        table_fqn,
                    }
                }).collect()
        }).unwrap();
}

#[derive(Debug, PartialEq, Eq)]
pub struct TableOutlineResult {
    pub table_name: String,
    pub table_comment: Option<String>,
    pub table_fqn: String,
}
