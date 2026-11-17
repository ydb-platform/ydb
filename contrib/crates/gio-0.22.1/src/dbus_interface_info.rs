// Take a look at the license at the top of the repository in the LICENSE file.

use std::ffi::CStr;

use crate::DBusInterfaceInfo;

impl DBusInterfaceInfo {
    pub fn name(&self) -> &str {
        unsafe {
            let c_obj = self.as_ptr();
            let name = (*c_obj).name;
            assert!(!name.is_null());
            let c_str = CStr::from_ptr(name);
            c_str.to_str().unwrap()
        }
    }
}
