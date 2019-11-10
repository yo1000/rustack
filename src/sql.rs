extern crate mysql;
extern crate r2d2;
extern crate r2d2_mysql;

use r2d2_mysql::MysqlConnectionManager;
use self::r2d2::PooledConnection;

use itertools::Itertools;
use std::collections::HashMap;

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

pub fn query_table(
    mut conn: PooledConnection<MysqlConnectionManager>,
    db_name: String,
    table_name: String,
) -> Vec<TableDetails> {
    return conn.prep_exec(r#"
            SELECT
                tbl.table_name                AS out_table_name,
                tbl.table_comment             AS out_table_comment,
                CONCAT(
                    tbl.table_schema, '.',
                    tbl.table_name
                )                             AS out_table_fqn,
                tbl.table_rows                AS out_table_rows,
                col.column_name               AS out_column_name,
                col.column_comment            AS out_column_comment,
                CONCAT(
                    tbl.table_schema, '.',
                    tbl.table_name  , '.',
                    col.column_name
                )                             AS out_column_fqn,
                col.column_type               AS out_column_sql_type,
                col.is_nullable               AS out_column_nullable,
                col.column_default            AS out_column_default,
                parent.referenced_table_name  AS out_parent_table_name,
                parent.referenced_column_name AS out_parent_column_name,
                child.table_name              AS out_child_table_name,
                child.column_name             AS out_child_column_name
            FROM
                information_schema.tables tbl
            INNER JOIN
                information_schema.columns col
                ON  tbl.table_schema = col.table_schema
                AND tbl.table_name = col.table_name
            LEFT OUTER JOIN
                information_schema.key_column_usage parent
                ON  col.table_schema = parent.table_schema
                AND col.table_name = parent.table_name
                AND col.column_name = parent.column_name
            LEFT OUTER JOIN
                information_schema.key_column_usage child
                ON  col.table_schema = child.table_schema
                AND col.table_name = child.referenced_table_name
                AND col.column_name = child.referenced_column_name
            WHERE
                tbl.table_schema = :in_db_name
            AND tbl.table_name = :in_table_name
            AND tbl.table_type = 'BASE TABLE'
            ORDER BY
                col.ordinal_position
    "#, params!{
            "in_db_name" => db_name,
            "in_table_name" => table_name,
    }).map::<Vec<TableDetails>, _>(|query_result| {
        let mapped_flat_table_details: HashMap<String, Vec<FlatTableDetails>> = query_result
            .map( |result| result.unwrap())
            .map(|row| {
                let mut r = row;
                FlatTableDetails {
                    table_name          : r.take("out_table_name").unwrap(),
                    table_comment       : r.take("out_table_comment"),
                    table_fqn           : r.take("out_table_fqn").unwrap(),
                    table_rows          : r.take("out_table_rows").unwrap(),
                    column_name         : r.take("out_column_name").unwrap(),
                    column_comment      : r.take("out_column_comment"),
                    column_fqn          : r.take("out_column_fqn").unwrap(),
                    column_sql_type     : r.take("out_column_sql_type").unwrap(),
                    column_nullable     : r.take("out_column_nullable").unwrap(),
                    column_default      : r.take("out_column_default"),
                    parent_table_name   : r.take("out_parent_table_name"),
                    parent_column_name  : r.take("out_parent_column_name"),
                    child_table_name    : r.take("out_child_table_name"),
                    child_column_name   : r.take("out_child_column_name"),
                }
            })
            .map(|details: FlatTableDetails| {
                let d = details.clone();
                (d.clone().table_fqn, d)
            })
            .into_group_map();

        mapped_flat_table_details
            .into_iter()
            .map(|(k, v)| {
                let first = v.get(0).unwrap();
                let f = first.clone();
                let parent = match (f.parent_table_name, f.parent_column_name) {
                    (Some(table_name), Some(column_name)) => Some(Relation {
                        table_name,
                        column_name,
                    }),
                    _ => None
                };
                let children = v.iter().map(|flat| {
                    let f = flat.clone();
                    match (f.child_table_name, f.child_column_name) {
                        (Some(table_name), Some(column_name)) => {
                            Some(Relation {
                                table_name,
                                column_name,
                            })
                        },
                        _ => None
                    } })
                    .filter(|relation| match relation {
                        Some(_) => true,
                        _ => false
                    })
                    .map(|relation| relation.unwrap())
                    .collect();

                TableDetails {
                    table_name: f.table_name,
                    table_comment: f.table_comment,
                    table_fqn: f.table_fqn,
                    table_rows: f.table_rows,
                    columns: v.iter().map(|flat| {
                        let f = flat.clone();
                        Column {
                            column_name: f.column_name,
                            column_comment: f.column_comment,
                            column_fqn: f.column_fqn,
                            column_sql_type: f.column_sql_type,
                            column_nullable: f.column_nullable,
                            column_default: f.column_default,
                        }
                    }).collect(),
                    parent,
                    children,
                }
            })
            .collect()
    }).unwrap();
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct FlatTableDetails {
    pub table_name: String,
    pub table_comment: Option<String>,
    pub table_fqn: String,
    pub table_rows: u64,
    pub column_name: String,
    pub column_comment: Option<String>,
    pub column_fqn: String,
    pub column_sql_type: String,
    pub column_nullable: String,
    pub column_default: Option<String>,
    pub parent_table_name: Option<String>,
    pub parent_column_name: Option<String>,
    pub child_table_name: Option<String>,
    pub child_column_name: Option<String>,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct TableDetails {
    pub table_name: String,
    pub table_comment: Option<String>,
    pub table_fqn: String,
    pub table_rows: u64,
    pub columns: Vec<Column>,
    pub parent: Option<Relation>,
    pub children: Vec<Relation>,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Column {
    pub column_name: String,
    pub column_comment: Option<String>,
    pub column_fqn: String,
    pub column_sql_type: String,
    pub column_nullable: String,
    pub column_default: Option<String>,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Relation {
    pub table_name: String,
    pub column_name: String,
}

#[derive(Debug, PartialEq, Eq)]
pub struct TableOutlineResult {
    pub table_name: String,
    pub table_comment: Option<String>,
    pub table_fqn: String,
}
