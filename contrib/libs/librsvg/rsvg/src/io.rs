//! Utilities to acquire streams and data from from URLs.

use data_url::{DataUrl, mime::Mime};
use gio::{
    Cancellable, File as GFile, InputStream, MemoryInputStream,
    prelude::{FileExt, FileExtManual},
};
use glib::{self, Bytes as GBytes, object::Cast};
use std::fmt;

use crate::url_resolver::AllowedUrl;

pub enum IoError {
    BadDataUrl,
    Glib(glib::Error),
}

impl From<glib::Error> for IoError {
    fn from(e: glib::Error) -> IoError {
        IoError::Glib(e)
    }
}

impl fmt::Display for IoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            IoError::BadDataUrl => write!(f, "invalid data: URL"),
            IoError::Glib(ref e) => e.fmt(f),
        }
    }
}

pub struct BinaryData {
    pub data: Vec<u8>,
    pub mime_type: Option<Mime>,
}

fn decode_data_uri(uri: &str) -> Result<BinaryData, IoError> {
    let data_url = DataUrl::process(uri).map_err(|_| IoError::BadDataUrl)?;

    let mime = data_url.mime_type();

    // data_url::mime::Mime doesn't impl Clone, so do it by hand

    let mime_type = Mime {
        type_: mime.type_.clone(),
        subtype: mime.subtype.clone(),
        parameters: mime.parameters.clone(),
    };

    let (bytes, fragment_id) = data_url.decode_to_vec().map_err(|_| IoError::BadDataUrl)?;

    // See issue #377 - per the data: URL spec
    // (https://fetch.spec.whatwg.org/#data-urls), those URLs cannot
    // have fragment identifiers.  So, just return an error if we find
    // one.  This probably indicates mis-quoted SVG data inside the
    // data: URL.
    if fragment_id.is_some() {
        return Err(IoError::BadDataUrl);
    }

    Ok(BinaryData {
        data: bytes,
        mime_type: Some(mime_type),
    })
}

/// Creates a stream for reading.  The url can be a data: URL or a plain URI.
pub fn acquire_stream(
    aurl: &AllowedUrl,
    cancellable: Option<&Cancellable>,
) -> Result<InputStream, IoError> {
    let uri = aurl.as_str();

    if uri.starts_with("data:") {
        let BinaryData { data, .. } = decode_data_uri(uri)?;

        //        {
        //            use std::fs::File;
        //            use std::io::prelude::*;
        //
        //            let mut file = File::create("data.bin").unwrap();
        //            file.write_all(&data).unwrap();
        //        }

        let stream = MemoryInputStream::from_bytes(&GBytes::from_owned(data));
        Ok(stream.upcast::<InputStream>())
    } else {
        let file = GFile::for_uri(uri);
        let stream = file.read(cancellable)?;

        Ok(stream.upcast::<InputStream>())
    }
}

/// Reads the entire contents pointed by an URL.  The url can be a data: URL or a plain URI.
pub fn acquire_data(
    aurl: &AllowedUrl,
    cancellable: Option<&Cancellable>,
) -> Result<BinaryData, IoError> {
    let uri = aurl.as_str();

    if uri.starts_with("data:") {
        Ok(decode_data_uri(uri)?)
    } else {
        let file = GFile::for_uri(uri);
        let (contents, _etag) = file.load_contents(cancellable)?;

        Ok(BinaryData {
            data: contents.to_vec(),
            mime_type: None,
        })
    }
}
