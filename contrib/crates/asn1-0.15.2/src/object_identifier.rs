use crate::base128;
use crate::parser::{ParseError, ParseErrorKind, ParseResult};
use alloc::fmt;

const MAX_OID_LENGTH: usize = 63;

/// Represents an ASN.1 `OBJECT IDENTIFIER`. ObjectIdentifiers are opaque, the only thing may be
/// done with them is test if they are equal to another `ObjectIdentifier`. The generally
/// recommended practice for handling them is to create some `ObjectIdentifier` constants with
/// `asn1::oid!()` and then compare ObjectIdentifiers you get from parsing to
/// those.
///
/// `asn1::oid!()` takes a series of arcs, for example: `asn1::oid!(1, 2, 3)`.
///
/// rust-asn1 stores `ObjectIdentifier`s in a fixed-size buffer, therefore
/// they are limited to OID values whose DER encoding fits into that buffer.
/// This buffer is sufficiently large to fit all known publically known OIDs,
/// so this should not effect most people.
#[derive(PartialEq, Eq, Clone, Hash)]
pub struct ObjectIdentifier {
    // Store the OID as DER encoded.
    der_encoded: [u8; MAX_OID_LENGTH],
    der_encoded_len: u8,
}

impl ObjectIdentifier {
    /// Parses an OID from a dotted string, e.g. `"1.2.840.113549"`.
    pub fn from_string(oid: &str) -> Option<ObjectIdentifier> {
        let mut parts = oid.split('.');

        let first = parts.next()?.parse::<u32>().ok()?;
        let second = parts.next()?.parse::<u32>().ok()?;
        if first > 2 || (first < 2 && second >= 40) {
            return None;
        }

        let mut der_data = [0; MAX_OID_LENGTH];
        let mut der_data_len = 0;
        der_data_len +=
            base128::write_base128_int(&mut der_data[der_data_len..], 40 * first + second)?;
        for part in parts {
            der_data_len += base128::write_base128_int(
                &mut der_data[der_data_len..],
                part.parse::<u32>().ok()?,
            )?;
        }
        Some(ObjectIdentifier {
            der_encoded: der_data,
            der_encoded_len: der_data_len as u8,
        })
    }

    /// Creates an `ObjectIdentifier` from its DER representation. This does
    /// not perform any allocations or copies.
    pub fn from_der(data: &[u8]) -> ParseResult<ObjectIdentifier> {
        if data.is_empty() {
            return Err(ParseError::new(ParseErrorKind::InvalidValue));
        } else if data.len() > MAX_OID_LENGTH {
            return Err(ParseError::new(ParseErrorKind::OidTooLong));
        }

        let mut parsed = (0, data);
        while !parsed.1.is_empty() {
            parsed = base128::read_base128_int(parsed.1)?;
        }

        let mut storage = [0; MAX_OID_LENGTH];
        storage[..data.len()].copy_from_slice(data);

        Ok(ObjectIdentifier {
            der_encoded: storage,
            der_encoded_len: data.len() as u8,
        })
    }

    /// Creates an `ObjectIdentifier` from its DER representation. Does not
    /// check that the DER is valid. Intended only for use from the `oid!()`
    /// macro. Do not use yourself!
    #[doc(hidden)]
    pub const fn from_der_unchecked(data: [u8; MAX_OID_LENGTH], data_len: u8) -> ObjectIdentifier {
        ObjectIdentifier {
            der_encoded: data,
            der_encoded_len: data_len,
        }
    }

    pub(crate) fn as_der(&self) -> &[u8] {
        &self.der_encoded[..self.der_encoded_len as usize]
    }
}

struct OidFormatter<'a>(&'a ObjectIdentifier);

impl fmt::Debug for OidFormatter<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut parsed = (0, self.0.as_der());

        parsed = base128::read_base128_int(parsed.1).unwrap();
        if parsed.0 < 80 {
            write!(f, "{}.{}", parsed.0 / 40, parsed.0 % 40)?;
        } else {
            write!(f, "2.{}", parsed.0 - 80)?;
        }

        while !parsed.1.is_empty() {
            parsed = base128::read_base128_int(parsed.1).unwrap();
            write!(f, ".{}", parsed.0)?;
        }

        Ok(())
    }
}

impl fmt::Debug for ObjectIdentifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ObjectIdentifier")
            .field("oid", &OidFormatter(self))
            .finish()
    }
}

impl fmt::Display for ObjectIdentifier {
    /// Converts an `ObjectIdentifier` to a dotted string, e.g.
    /// "1.2.840.113549".
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&OidFormatter(self), f)
    }
}

#[cfg(test)]
mod tests {
    use super::MAX_OID_LENGTH;
    use crate::{ObjectIdentifier, ParseError, ParseErrorKind};
    use alloc::format;
    use alloc::string::ToString;

    #[test]
    fn test_object_identifier_from_string() {
        for val in &[
            "",
            "1",
            "3.10",
            "1.50",
            "2.12.a3.4",
            "a.4",
            "1.a",
            ".2.5",
            "2..5",
            "2.5.",
            "1.3.6.1.4.1.1248.1.1.2.1.3.21.69.112.115.111.110.32.83.116.121.108.117.115.32.80.114.111.32.52.57.48.48.123.124412.31.213321.123.110.32.83.116.121.108.117.115.32.80.114.111.32.52.57.48.48.123.124412.31.213321.123",
        ] {
            assert_eq!(ObjectIdentifier::from_string(val), None);
        }

        for val in &[
            "2.5",
            "2.5.2",
            "1.2.840.113549",
            "1.2.3.4",
            "1.2.840.133549.1.1.5",
            "2.100.3",
        ] {
            assert!(ObjectIdentifier::from_string(val).is_some());
        }
    }

    #[test]
    fn test_from_der() {
        assert_eq!(ObjectIdentifier::from_der(b"\x06\x40\x2b\x06\x01\x04\x01\x89\x60\x01\x01\x02\x01\x03\x15\x45\x70\x73\x6f\x6e\x20\x53\x74\x79\x6c\x75\x73\x20\x50\x72\x6f\x20\x34\x39\x30\x30\x7b\x87\xcb\x7c\x1f\x8d\x82\x49\x7b\x2b\x06\x01\x04\x01\x89\x60\x01\x01\x02\x01\x03\x15\x45\x70\x73\x6f\x6e\x20"), Err(ParseError::new(ParseErrorKind::OidTooLong)));
    }

    #[test]
    fn test_from_der_unchecked() {
        for (dotted_string, der) in &[("2.5", b"\x55" as &[u8]), ("2.100.3", b"\x81\x34\x03")] {
            let mut data = [0; MAX_OID_LENGTH];
            data[..der.len()].copy_from_slice(der);
            assert_eq!(
                ObjectIdentifier::from_string(dotted_string).unwrap(),
                ObjectIdentifier::from_der_unchecked(data, der.len() as u8)
            );
        }
    }

    #[test]
    fn test_debug() {
        let oid = ObjectIdentifier::from_string("1.2.3.4").unwrap();
        assert_eq!(format!("{:?}", oid), "ObjectIdentifier { oid: 1.2.3.4 }");
    }

    #[test]
    fn test_to_string() {
        for val in &[
            "0.4",
            "2.5",
            "2.5.2",
            "1.2.840.113549",
            "1.2.3.4",
            "1.2.840.133549.1.1.5",
            "2.100.3",
        ] {
            assert_eq!(
                &ObjectIdentifier::from_string(val).unwrap().to_string(),
                val
            );
        }
    }
}
