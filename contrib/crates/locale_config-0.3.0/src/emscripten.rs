use std::ffi::CStr;
use std::os::raw::c_char;
use std::os::raw::c_int;

use super::Locale;

// Bind some emscripten functions. Copied from webplatform crate.
extern "C" {
    pub fn emscripten_asm_const_int(s: *const c_char, ...) -> c_int;
}

pub fn system_locale() -> Option<Locale> {
    const JS: &'static [u8] = b"\
        try { \
            return allocate(intArrayFromString(navigator.languages.join(',')), 'i8', ALLOC_STACK); \
        } catch(e) {} \
        try { \
            return allocate(intArrayFromString(navigator.language), 'i8', ALLOC_STACK); \
        } catch(e) {} \
        try { \
            return allocate(intArrayFromString(navigator.userLanguage), 'i8', ALLOC_STACK); \
        } catch(e) {} \
        return 0;\0";
    unsafe {
        let cptr = emscripten_asm_const_int(&JS[0] as *const _ as *const c_char);
        return CStr::from_ptr(cptr as *const c_char).to_str().ok()
            .and_then(|s| Locale::new(s).ok());
    }
}