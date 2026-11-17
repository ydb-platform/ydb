// Take a look at the license at the top of the repository in the LICENSE file.

use crate::{Analysis, Item};

impl Item {
    pub fn offset(&self) -> i32 {
        unsafe { (*self.as_ptr()).offset }
    }

    pub fn length(&self) -> i32 {
        unsafe { (*self.as_ptr()).length }
    }

    pub fn num_chars(&self) -> i32 {
        unsafe { (*self.as_ptr()).num_chars }
    }

    pub fn analysis(&self) -> &Analysis {
        unsafe { &*(&((*self.as_ptr()).analysis) as *const _ as *const Analysis) }
    }
}
