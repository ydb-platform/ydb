//! Inspect Unix system for locale configuration

use std::env;
use super::{LanguageRange,Locale};

fn tag(s: &str) -> super::Result<LanguageRange> {
    LanguageRange::from_unix(s).or_else(|_| LanguageRange::new(s))
}

// TODO: Read /etc/locale.alias
fn tag_inv(s: &str) -> LanguageRange {
    tag(s).unwrap_or(LanguageRange::invariant())
}

pub fn system_locale() -> Option<Locale> {
    // LC_ALL overrides everything
    if let Ok(all) = env::var("LC_ALL") {
        if let Ok(t) = tag(all.as_ref()) {
            return Some(Locale::from(t));
        }
    }
    // LANG is default
    let mut loc =
        if let Ok(lang) = env::var("LANG") {
            Locale::from(tag_inv(lang.as_ref()))
        } else {
            Locale::invariant()
        };
    // category overrides
    for &(cat, var) in [
        ("ctype",       "LC_CTYPE"),
        ("numeric",     "LC_NUMERIC"),
        ("time",        "LC_TIME"),
        ("collate",     "LC_COLLATE"),
        ("monetary",    "LC_MONETARY"),
        ("messages",    "LC_MESSAGES"),
        ("paper",       "LC_PAPER"),
        ("name",        "LC_NAME"),
        ("address",     "LC_ADDRESS"),
        ("telephone",   "LC_TELEPHONE"),
        ("measurement", "LC_MEASUREMENT"),
    ].iter() {
        if let Ok(val) = env::var(var) {
            if let Ok(tag) = tag(val.as_ref())
            {
                loc.add_category(cat, &tag);
            }
        }
    }
    // LANGUAGE defines fallbacks
    if let Ok(langs) = env::var("LANGUAGE") {
        for i in langs.split(':') {
            if i != "" {
                if let Ok(tag) = tag(i) {
                    loc.add(&tag);
                }
            }
        }
    }
    if loc.as_ref() != "" {
        return Some(loc);
    } else {
        return None;
    }
}
