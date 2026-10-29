extern crate locale_config;

pub fn main() {
    println!("{}", locale_config::Locale::user_default());
}
