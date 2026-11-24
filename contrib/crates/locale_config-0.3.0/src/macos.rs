//! Inspect macOS system for locale configuration
extern crate objc_foundation;

use super::{LanguageRange, Locale};

use objc::runtime::Object;
use self::objc_foundation::{INSString, NSString};

pub fn system_locale() -> Option<Locale> {
	let locale_identifier = unsafe {
		let nslocale = class!(NSLocale);
		let current_locale: *mut Object = msg_send![nslocale, currentLocale];
		let locale_identifier: *const NSString = msg_send![current_locale, localeIdentifier];
		locale_identifier.as_ref().unwrap()
	};
	Some(Locale::from(LanguageRange::from_unix(locale_identifier.as_str()).unwrap()))
}
