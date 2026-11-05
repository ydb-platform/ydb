//! Inspect CGI environment variables for locale configuration

use std::env;
use super::{Locale};

pub fn system_locale() -> Option<Locale> {
    if let Ok(al) = env::var("HTTP_ACCEPT_LANGUAGE") {
        Locale::new(al.as_ref()).ok()
    } else {
        None
    }
}
