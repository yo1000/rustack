use std::{
    env,
    str::FromStr,
};

pub fn load<T>(name: &str, def_var: Option<T>) -> T where T: FromStr {
    let var: Result<String, _> = env::var(name);
    match var {
        Ok(v) => {
            let parsed = v.parse::<T>();
            match parsed {
                Ok(p) => p,
                _ => def_var.expect(format!("{} must be set", name).as_str()),
            }
        },
        _ => def_var.expect(format!("{} must be set", name).as_str()),
    }
}
