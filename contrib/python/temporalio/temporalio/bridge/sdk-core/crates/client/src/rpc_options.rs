use crate::{RetryOptions, request_extensions::RetryConfigForCall};
use std::time::Duration;
use tonic::metadata::{
    AsciiMetadataKey, AsciiMetadataValue, BinaryMetadataKey, BinaryMetadataValue, KeyAndValueRef,
    MetadataMap,
};

/// Metadata attached to a single high-level client RPC.
///
/// Per-call values take precedence over metadata configured on the connection.
#[derive(Clone, Debug, Default)]
pub struct RpcMetadata {
    inner: MetadataMap,
}

impl RpcMetadata {
    /// Create empty RPC metadata.
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert an ASCII metadata value, returning the previous value for the key.
    pub fn insert(
        &mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<Option<String>, RpcMetadataError> {
        let key = key.into();
        let value = value.into();
        let parsed_key = key
            .parse::<AsciiMetadataKey>()
            .map_err(|_| RpcMetadataError::InvalidAsciiKey { key: key.clone() })?;
        let parsed_value = value.parse::<AsciiMetadataValue>().map_err(|_| {
            RpcMetadataError::InvalidAsciiValue {
                key,
                value: value.clone(),
            }
        })?;
        Ok(self.inner.insert(parsed_key, parsed_value).map(|previous| {
            previous
                .to_str()
                .expect("ASCII RPC metadata remains valid while stored")
                .to_owned()
        }))
    }

    /// Insert a binary metadata value, returning the previous value for the key.
    pub fn insert_binary(
        &mut self,
        key: impl Into<String>,
        value: impl Into<Vec<u8>>,
    ) -> Result<Option<Vec<u8>>, RpcMetadataError> {
        let key = key.into();
        let parsed_key = key
            .parse::<BinaryMetadataKey>()
            .map_err(|_| RpcMetadataError::InvalidBinaryKey { key })?;
        Ok(self
            .inner
            .insert_bin(parsed_key, BinaryMetadataValue::from_bytes(&value.into()))
            .map(|previous| {
                previous
                    .to_bytes()
                    .expect("binary RPC metadata remains valid while stored")
                    .to_vec()
            }))
    }

    /// Get an ASCII metadata value.
    pub fn get_ascii(&self, key: &str) -> Option<&str> {
        self.inner.get(key).and_then(|value| value.to_str().ok())
    }

    /// Get a binary metadata value.
    pub fn get_binary(&self, key: &str) -> Option<Vec<u8>> {
        self.inner
            .get_bin(key)
            .and_then(|value| value.to_bytes().ok())
            .map(|value| value.to_vec())
    }

    /// Remove an ASCII metadata value.
    pub fn remove_ascii(&mut self, key: &str) -> Option<String> {
        self.inner.remove(key).map(|value| {
            value
                .to_str()
                .expect("ASCII RPC metadata remains valid while stored")
                .to_owned()
        })
    }

    /// Remove a binary metadata value.
    pub fn remove_binary(&mut self, key: &str) -> Option<Vec<u8>> {
        self.inner.remove_bin(key).map(|value| {
            value
                .to_bytes()
                .expect("binary RPC metadata remains valid while stored")
                .to_vec()
        })
    }

    /// Iterate over ASCII metadata values.
    pub fn ascii(&self) -> impl Iterator<Item = (&str, &str)> {
        self.inner.iter().filter_map(|entry| match entry {
            KeyAndValueRef::Ascii(key, value) => Some((
                key.as_str(),
                value
                    .to_str()
                    .expect("ASCII RPC metadata remains valid while stored"),
            )),
            KeyAndValueRef::Binary(_, _) => None,
        })
    }

    /// Iterate over binary metadata values.
    pub fn binary(&self) -> impl Iterator<Item = (&str, Vec<u8>)> {
        self.inner.iter().filter_map(|entry| match entry {
            KeyAndValueRef::Ascii(_, _) => None,
            KeyAndValueRef::Binary(key, value) => Some((
                key.as_str(),
                value
                    .to_bytes()
                    .expect("binary RPC metadata remains valid while stored")
                    .to_vec(),
            )),
        })
    }

    fn apply_to<T>(&self, request: &mut tonic::Request<T>) {
        for entry in self.inner.iter() {
            match entry {
                KeyAndValueRef::Ascii(key, value) => {
                    request.metadata_mut().insert(key.clone(), value.clone());
                }
                KeyAndValueRef::Binary(key, value) => {
                    request
                        .metadata_mut()
                        .insert_bin(key.clone(), value.clone());
                }
            }
        }
    }
}

impl PartialEq for RpcMetadata {
    fn eq(&self, other: &Self) -> bool {
        self.inner.as_ref() == other.inner.as_ref()
    }
}

impl Eq for RpcMetadata {}

/// An invalid key or value supplied to [`RpcMetadata`].
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum RpcMetadataError {
    /// The key is not valid ASCII gRPC metadata.
    #[error("invalid ASCII RPC metadata key: {key}")]
    InvalidAsciiKey {
        /// The invalid key.
        key: String,
    },
    /// The value is not valid ASCII gRPC metadata.
    #[error("invalid ASCII RPC metadata value for key {key}: {value:?}")]
    InvalidAsciiValue {
        /// The associated key.
        key: String,
        /// The invalid value.
        value: String,
    },
    /// The key is not valid binary gRPC metadata.
    #[error("invalid binary RPC metadata key: {key}")]
    InvalidBinaryKey {
        /// The invalid key.
        key: String,
    },
}

/// Controls applied to a single high-level client RPC.
#[derive(Clone, Debug, PartialEq, bon::Builder)]
#[non_exhaustive]
pub struct RpcOptions {
    /// Metadata attached to the RPC.
    #[builder(default)]
    pub metadata: RpcMetadata,
    /// Timeout for the RPC, overriding the connection's default deadline.
    pub timeout: Option<Duration>,
    /// Retry behavior for the RPC, overriding the connection's retry configuration.
    pub retry_options: Option<RetryOptions>,
}

impl Default for RpcOptions {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl RpcOptions {
    pub(crate) fn apply_to<T>(&self, request: &mut tonic::Request<T>) {
        self.metadata.apply_to(request);
        if let Some(timeout) = self.timeout {
            request.set_timeout(timeout);
        }
        if let Some(retry_options) = &self.retry_options {
            request
                .extensions_mut()
                .insert(RetryConfigForCall(retry_options.clone()));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_validates_and_exposes_values() {
        let mut metadata = RpcMetadata::new();
        assert_eq!(metadata.insert("trace-id", "first").unwrap(), None);
        assert_eq!(
            metadata.insert("trace-id", "second").unwrap(),
            Some("first".to_owned())
        );
        assert_eq!(
            metadata
                .insert_binary("trace-data-bin", vec![0, 255])
                .unwrap(),
            None
        );
        assert_eq!(metadata.get_ascii("trace-id"), Some("second"));
        assert_eq!(metadata.get_binary("trace-data-bin"), Some(vec![0, 255]));
        assert!(matches!(
            metadata.insert("bad key", "value"),
            Err(RpcMetadataError::InvalidAsciiKey { .. })
        ));
        assert!(matches!(
            metadata.insert("valid-key", "bad\nvalue"),
            Err(RpcMetadataError::InvalidAsciiValue { .. })
        ));
        assert!(matches!(
            metadata.insert_binary("missing-suffix", vec![]),
            Err(RpcMetadataError::InvalidBinaryKey { .. })
        ));
        assert_eq!(metadata.remove_ascii("trace-id"), Some("second".to_owned()));
        assert_eq!(metadata.remove_binary("trace-data-bin"), Some(vec![0, 255]));
    }
}
