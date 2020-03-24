extern crate mysql;
extern crate r2d2;
extern crate r2d2_mysql;

use std::collections::HashMap;

use itertools::Itertools;
use mysql::{
    prelude::FromValue,
    Row
};
use r2d2_mysql::MysqlConnectionManager;

use self::r2d2::PooledConnection;

pub fn query_table_outline(
    conn: &mut PooledConnection<MysqlConnectionManager>,
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

pub fn query_table_size_map(
    conn: &mut PooledConnection<MysqlConnectionManager>,
    param: String
) -> HashMap<String, TableSizeResult> {
    return conn.prep_exec(r#"
            SELECT
                tbl.table_name          AS table_name,
                count(col.column_name)  AS column_count,
                tbl.table_rows          AS row_count
            FROM
                information_schema.tables tbl
            INNER JOIN
                information_schema.columns col
                    ON  tbl.table_schema  = col.table_schema
                    AND tbl.table_name    = col.table_name
            WHERE
                tbl.table_schema  = :param_schema_name
            AND tbl.table_type    = 'BASE TABLE'
            GROUP BY
                tbl.table_name,
                tbl.table_rows
            "#, params!{
                "param_schema_name" => param
            })
        .map::<HashMap<String, TableSizeResult>, _>(|result| {
            let size_result_vec: Vec<_> = result
                .map(|x| x.unwrap())
                .map(|row| {
                    let (table_name, columns, rows) = mysql::from_row(row);
                    (table_name, TableSizeResult {
                        columns,
                        rows,
                    })
                }).collect_vec();

            size_result_vec.to_vec().into_iter().collect()
        }).unwrap();
}

pub fn query_table_referencing_count_to_parent_map(
    conn: &mut PooledConnection<MysqlConnectionManager>,
    param: String
) -> HashMap<String, u32> {
    return conn.prep_exec(r#"
            SELECT
                table_name      AS table_name,
                sum(col_count)  AS ref_count
            FROM
                (
                    SELECT
                        table_name,
                        count(referenced_column_name) AS col_count
                    FROM
                        information_schema.key_column_usage
                    WHERE
                        table_schema = :param_schema_name
                    GROUP BY
                        table_name, referenced_table_name
                ) parent_col
            GROUP BY
                table_name
            "#, params!{
                "param_schema_name" => param
            })
        .map::<HashMap<String, u32>, _>(|result| {
            let size_result_vec: Vec<_> = result
                .map(|x| x.unwrap())
                .map(|row| {
                    let (table_name, ref_count) = mysql::from_row(row);
                    (table_name, ref_count)
                }).collect_vec();

            size_result_vec.to_vec().into_iter().collect()
        }).unwrap();
}

pub fn query_table_referenced_count_from_children_map(
    conn: &mut PooledConnection<MysqlConnectionManager>,
    param: String
) -> HashMap<String, u32> {
    return conn.prep_exec(r#"
            SELECT
                table_name AS table_name,
                sum(count) AS ref_count
            FROM
                (
                    SELECT
                        referenced_table_name   AS table_name,
                        count(column_name)      AS count
                    FROM
                        information_schema.key_column_usage
                    WHERE
                        table_schema = :param_schema_name
                    AND referenced_table_name IS NOT NULL
                    GROUP BY
                        referenced_table_name, table_name
                ) child_col
            GROUP BY
                table_name
            "#, params!{
                "param_schema_name" => param
            })
        .map::<HashMap<String, u32>, _>(|result| {
            let size_result_vec: Vec<_> = result
                .map(|x| x.unwrap())
                .map(|row| {
                    let (table_name, ref_count) = mysql::from_row(row);
                    (table_name, ref_count)
                }).collect_vec();

            size_result_vec.to_vec().into_iter().collect()
        }).unwrap();
}

fn query_flat_table(
    conn: &mut PooledConnection<MysqlConnectionManager>,
    db_name: &String,
    table_name: &String,
) -> Vec<FlatTable> {
    conn.prep_exec(r#"
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
            col.column_default            AS out_column_default
        FROM
            information_schema.tables tbl
        INNER JOIN
            information_schema.columns col
            ON  tbl.table_schema = col.table_schema
            AND tbl.table_name = col.table_name
        WHERE
            tbl.table_schema = :in_db_name
        AND tbl.table_name = :in_table_name
        AND tbl.table_type = 'BASE TABLE'
        ORDER BY
            col.ordinal_position
    "#, params!{
        "in_db_name" => db_name,
        "in_table_name" => table_name,
    }).map::<Vec<FlatTable>, _>(|query_result| {
        query_result
            .map(|result| result.unwrap())
            .map(|r| {
                FlatTable {
                    table_name: take_val::<String>(&r, "out_table_name"),
                    table_comment: take_nullable_val::<String>(&r, "out_table_comment"),
                    table_fqn: take_val::<String>(&r, "out_table_fqn"),
                    table_rows: take_val::<u64>(&r, "out_table_rows"),
                    column_name: take_val::<String>(&r, "out_column_name"),
                    column_comment: take_nullable_val::<String>(&r, "out_column_comment"),
                    column_fqn: take_val::<String>(&r, "out_column_fqn"),
                    column_sql_type: take_val::<String>(&r, "out_column_sql_type"),
                    column_nullable: take_val::<String>(&r, "out_column_nullable"),
                    column_default: take_nullable_val::<String>(&r, "out_column_default"),
                }
            })
            .collect()
    }).unwrap()
}

fn query_column_parent(
    conn: &mut PooledConnection<MysqlConnectionManager>,
    db_name: &String,
    table_name: &String,
    column_name: &String,
) -> Option<Relation> {
    let parent_result = conn.prep_exec(r#"
        SELECT
            parent.referenced_table_name  AS out_parent_table_name,
            parent.referenced_column_name AS out_parent_column_name
        FROM
            information_schema.key_column_usage parent
        WHERE
            parent.table_schema = :in_db_name
        AND parent.table_name   = :in_table_name
        AND parent.column_name  = :in_column_name
        ORDER BY
            parent.referenced_table_name ASC,
            parent.referenced_column_name ASC
    "#, params!{
        "in_db_name" => db_name,
        "in_table_name" => table_name,
        "in_column_name" => column_name,
    }).map::<Option<Relation>, _>(|query_result| {
        let rel: Vec<Option<Relation>> = query_result
            .map(|result| result.unwrap())
            .map(|r| {
                let parent_table_name = take_nullable_val::<String>(&r, "out_parent_table_name");
                let parent_column_name = take_nullable_val::<String>(&r, "out_parent_column_name");

                match (parent_table_name, parent_column_name) {
                    (Some(t), Some(c)) => Some(Relation {
                        table_name: t,
                        column_name: c,
                    }),
                    _ => None,
                }
            }).collect();

        match rel.first() {
            Some(r) => r.clone(),
            _ => None
        }
    });

    match parent_result {
        Ok(t) => t.clone(),
        _ => None,
    }
}

fn query_column_children(
    conn: &mut PooledConnection<MysqlConnectionManager>,
    db_name: &String,
    table_name: &String,
    column_name: &String,
) -> Vec<Relation> {
    let children_result = conn.prep_exec(r#"
        SELECT
            child.table_name    AS out_child_table_name,
            child.column_name   AS out_child_column_name
        FROM
            information_schema.key_column_usage child
        WHERE
                child.table_schema              = :in_db_name
            AND child.referenced_table_name     = :in_table_name
            AND child.referenced_column_name    = :in_column_name
        ORDER BY
            child.table_name ASC,
            child.column_name ASC
    "#, params!{
        "in_db_name" => db_name,
        "in_table_name" => table_name,
        "in_column_name" => column_name,
    }).map::<Vec<Relation>, _>(|query_result| {
        query_result
            .map(|result| result.unwrap())
            .map(|r| {
                Relation {
                    table_name: take_val::<String>(&r, "out_child_table_name"),
                    column_name: take_val::<String>(&r, "out_child_column_name")
                }
            }).collect()
    });

    match children_result {
        Ok(t) => t.clone(),
        _ => vec![],
    }
}

pub fn query_table(
    mut conn: PooledConnection<MysqlConnectionManager>,
    db_name: String,
    table_name: String,
) -> Option<Table> {
    let flat_tables = query_flat_table(&mut conn, &db_name, &table_name);

    let first = match flat_tables.first() {
        Some(t) => t.clone(),
        _ => return None,
    };

    Some(Table {
        table_name: first.table_name,
        table_comment: first.table_comment,
        table_fqn: first.table_fqn,
        table_rows: first.table_rows,
        table_columns: flat_tables.iter().map(|flat| {
            let f = flat.clone();
            let column_name = f.column_name;
            let parent = query_column_parent(&mut conn, &db_name, &table_name, &column_name);
            let children = query_column_children(&mut conn, &db_name, &table_name, &column_name);
            Column {
                column_name,
                column_comment: f.column_comment,
                column_fqn: f.column_fqn,
                column_sql_type: f.column_sql_type,
                column_nullable: f.column_nullable,
                column_default: f.column_default,
                column_parent: parent,
                column_children: children,
            }
        }).collect(),
    })
}

fn take_val<T>(row: &Row, index: &str) -> T where T: FromValue {
    row.get::<T, &str>(index).unwrap()
}

fn take_nullable_val<T>(row: &Row, index: &str) -> Option<T> where T: FromValue {
    match row.get_opt::<T, &str>(index) {
        Some(a) => {
            match a {
                Ok(b) => Some(b),
                _ => None,
            }
        },
        _ => None,
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct TableRelations {
    pub table: Table,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct Table {
    pub table_name: String,
    pub table_comment: Option<String>,
    pub table_fqn: String,
    pub table_rows: u64,
    pub table_columns: Vec<Column>,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct FlatTable {
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
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct Column {
    pub column_name: String,
    pub column_comment: Option<String>,
    pub column_fqn: String,
    pub column_sql_type: String,
    pub column_nullable: String,
    pub column_default: Option<String>,
    pub column_parent: Option<Relation>,
    pub column_children: Vec<Relation>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
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

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct TableSizeResult {
    pub columns: u32,
    pub rows: u64,
}
