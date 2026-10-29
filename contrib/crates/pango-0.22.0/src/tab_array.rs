// Take a look at the license at the top of the repository in the LICENSE file.

use glib::{Slice, translate::*};

use crate::{TabAlign, TabArray};

impl TabArray {
    #[doc(alias = "pango_tab_array_get_tabs")]
    #[doc(alias = "get_tabs")]
    pub fn tabs(&self) -> (Vec<TabAlign>, Slice<i32>) {
        let size = self.size() as usize;
        unsafe {
            let mut alignments = std::mem::MaybeUninit::uninit();
            let mut locations = std::mem::MaybeUninit::uninit();
            crate::ffi::pango_tab_array_get_tabs(
                mut_override(self.to_glib_none().0),
                alignments.as_mut_ptr(),
                locations.as_mut_ptr(),
            );
            let locations = Slice::from_glib_container_num(locations.assume_init(), size);
            let alignments = alignments.assume_init();
            let mut alignments_vec = Vec::with_capacity(locations.len());
            for i in 0..locations.len() {
                alignments_vec.push(from_glib(*alignments.add(i)));
            }
            (alignments_vec, locations)
        }
    }
}

#[cfg(feature = "v1_50")]
#[cfg_attr(docsrs, doc(cfg(feature = "v1_50")))]
impl std::str::FromStr for TabArray {
    type Err = glib::BoolError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_string(s)
    }
}

#[cfg(test)]
mod tests {
    use crate::{TabAlign, TabArray};
    #[test]
    fn tab_array_tabs() {
        let mut array = TabArray::new(4, false);
        for i in 0..4 {
            array.set_tab(i, TabAlign::Left, i * 10);
        }
        let (alignments, locations) = array.tabs();
        assert_eq!(alignments.len(), 4);
        assert_eq!(locations.len(), 4);
        for i in 0..alignments.len() {
            assert_eq!(alignments[i], TabAlign::Left);
            assert_eq!(locations[i], i as i32 * 10);
        }
    }
}
