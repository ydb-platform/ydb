//! Store XML element attributes and their values.

use std::slice;
use std::str;

use markup5ever::{
    LocalName, Namespace, Prefix, QualName, expanded_name, local_name, namespace_url, ns,
};
use string_cache::DefaultAtom;

use crate::error::{ImplementationLimit, LoadingError};
use crate::limits;
use crate::util::{opt_utf8_cstr, utf8_cstr, utf8_cstr_bounds};

/// Type used to store attribute values.
///
/// Attribute values are often repeated in an SVG file, so we intern them using the
/// string_cache crate.
pub type AttributeValue = DefaultAtom;

/// Iterable wrapper for libxml2's representation of attribute/value.
///
/// See the [`new_from_xml2_attributes`] function for information.
///
/// [`new_from_xml2_attributes`]: #method.new_from_xml2_attributes
#[derive(Clone)]
pub struct Attributes {
    attrs: Box<[(QualName, AttributeValue)]>,
    id_idx: Option<u16>,
    class_idx: Option<u16>,
}

/// Iterator from `Attributes.iter`.
pub struct AttributesIter<'a>(slice::Iter<'a, (QualName, AttributeValue)>);

#[cfg(test)]
impl Default for Attributes {
    fn default() -> Self {
        Self::new()
    }
}

impl Attributes {
    #[cfg(test)]
    pub fn new() -> Attributes {
        Attributes {
            attrs: [].into(),
            id_idx: None,
            class_idx: None,
        }
    }

    /// Creates an iterable `Attributes` from the C array of borrowed C strings.
    ///
    /// With libxml2's SAX parser, the caller's startElementNsSAX2Func
    /// callback gets passed a `xmlChar **` for attributes, which
    /// comes in groups of (localname/prefix/URI/value_start/value_end).
    /// In those, localname/prefix/URI are NUL-terminated strings;
    /// value_start and value_end point to the start-inclusive and
    /// end-exclusive bytes in the attribute's value.
    ///
    /// # Safety
    ///
    /// This function is unsafe because the caller must guarantee the following:
    ///
    /// * `attrs` is a valid pointer, with (n_attributes * 5) elements.
    ///
    /// * All strings are valid UTF-8.
    #[allow(unsafe_op_in_unsafe_fn)]
    pub unsafe fn new_from_xml2_attributes(
        n_attributes: usize,
        attrs: *const *const libc::c_char,
    ) -> Result<Attributes, LoadingError> {
        let mut array = Vec::with_capacity(n_attributes);
        let mut id_idx = None;
        let mut class_idx = None;

        if n_attributes > limits::MAX_LOADED_ATTRIBUTES {
            return Err(LoadingError::LimitExceeded(
                ImplementationLimit::TooManyAttributes,
            ));
        }

        if n_attributes > 0 && !attrs.is_null() {
            for attr in slice::from_raw_parts(attrs, n_attributes * 5).chunks_exact(5) {
                let localname = attr[0];
                let prefix = attr[1];
                let uri = attr[2];
                let value_start = attr[3];
                let value_end = attr[4];

                assert!(!localname.is_null());

                let localname = utf8_cstr(localname);

                let prefix = opt_utf8_cstr(prefix);
                let uri = opt_utf8_cstr(uri);
                let qual_name = QualName::new(
                    prefix.map(Prefix::from),
                    uri.map(Namespace::from)
                        .unwrap_or_else(|| namespace_url!("")),
                    LocalName::from(localname),
                );

                if !value_start.is_null() && !value_end.is_null() {
                    assert!(value_end >= value_start);

                    let value_str = utf8_cstr_bounds(value_start, value_end);
                    let value_atom = DefaultAtom::from(value_str);

                    let idx = array.len() as u16;
                    match qual_name.expanded() {
                        expanded_name!("", "id") => id_idx = Some(idx),
                        expanded_name!("", "class") => class_idx = Some(idx),
                        _ => (),
                    }

                    array.push((qual_name, value_atom));
                }
            }
        }

        Ok(Attributes {
            attrs: array.into(),
            id_idx,
            class_idx,
        })
    }

    /// Returns the number of attributes.
    pub fn len(&self) -> usize {
        self.attrs.len()
    }

    /// Creates an iterator that yields `(QualName, &'a str)` tuples.
    pub fn iter(&self) -> AttributesIter<'_> {
        AttributesIter(self.attrs.iter())
    }

    pub fn get_id(&self) -> Option<&str> {
        self.id_idx.and_then(|idx| {
            self.attrs
                .get(usize::from(idx))
                .map(|(_name, value)| &value[..])
        })
    }

    pub fn get_class(&self) -> Option<&str> {
        self.class_idx.and_then(|idx| {
            self.attrs
                .get(usize::from(idx))
                .map(|(_name, value)| &value[..])
        })
    }

    pub fn clear_class(&mut self) {
        self.class_idx = None;
    }
}

impl<'a> Iterator for AttributesIter<'a> {
    type Item = (QualName, &'a str);

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|(a, v)| (a.clone(), v.as_ref()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use markup5ever::{expanded_name, local_name, ns};
    use std::ffi::CString;
    use std::ptr;

    #[test]
    fn empty_attributes() {
        let map = unsafe { Attributes::new_from_xml2_attributes(0, ptr::null()).unwrap() };
        assert_eq!(map.len(), 0);
    }

    #[test]
    fn attributes_with_namespaces() {
        let attrs = [
            (
                CString::new("href").unwrap(),
                Some(CString::new("xlink").unwrap()),
                Some(CString::new("http://www.w3.org/1999/xlink").unwrap()),
                CString::new("1").unwrap(),
            ),
            (
                CString::new("ry").unwrap(),
                None,
                None,
                CString::new("2").unwrap(),
            ),
            (
                CString::new("d").unwrap(),
                None,
                None,
                CString::new("").unwrap(),
            ),
        ];

        let mut v: Vec<*const libc::c_char> = Vec::new();

        for (localname, prefix, uri, val) in &attrs {
            v.push(localname.as_ptr());
            v.push(
                prefix
                    .as_ref()
                    .map(|p: &CString| p.as_ptr())
                    .unwrap_or_else(ptr::null),
            );
            v.push(
                uri.as_ref()
                    .map(|p: &CString| p.as_ptr())
                    .unwrap_or_else(ptr::null),
            );

            let val_start = val.as_ptr();
            let val_end = unsafe { val_start.add(val.as_bytes().len()) };
            v.push(val_start); // value_start
            v.push(val_end); // value_end
        }

        let attrs = unsafe { Attributes::new_from_xml2_attributes(3, v.as_ptr()).unwrap() };

        let mut had_href: bool = false;
        let mut had_ry: bool = false;
        let mut had_d: bool = false;

        for (a, v) in attrs.iter() {
            match a.expanded() {
                expanded_name!(xlink "href") => {
                    assert!(v == "1");
                    had_href = true;
                }

                expanded_name!("", "ry") => {
                    assert!(v == "2");
                    had_ry = true;
                }

                expanded_name!("", "d") => {
                    assert!(v.is_empty());
                    had_d = true;
                }

                _ => unreachable!(),
            }
        }

        assert!(had_href);
        assert!(had_ry);
        assert!(had_d);
    }
}
