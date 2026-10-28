#![cfg_attr(docsrs, feature(doc_cfg))]
#![doc = include_str!("../README.md")]
#![warn(missing_docs)]

use std::sync::Arc;

#[cfg(feature = "dbg")]
use rustls::crypto::CryptoProvider;
#[cfg(feature = "dbg")]
use rustls::pki_types::CertificateDer;
use rustls::{client::WantsClientCert, ClientConfig, ConfigBuilder, WantsVerifier};

mod verification;
pub use verification::Verifier;

// Build the Android module when generating docs so that
// the Android-specific functions are included regardless of
// the host.
#[cfg(any(all(doc, docsrs), target_os = "android"))]
#[cfg_attr(docsrs, doc(cfg(target_os = "android")))]
pub mod android;

/// Fixures and data to support testing the server
/// certificate verifier.
#[cfg(any(test, feature = "ffi-testing"))]
mod tests;

// Re-export any exported functions that are required for
// tests to run in a platform-native environment.
#[cfg(feature = "ffi-testing")]
#[cfg_attr(feature = "ffi-testing", allow(unused_imports))]
pub use tests::ffi::*;

/// Exposed for debugging certificate issues with standalone tools.
///
/// This is not intended for production use, you should use [`BuilderVerifierExt`] or
/// [`ConfigVerifierExt`] instead.
#[cfg(feature = "dbg")]
pub fn verifier_for_dbg(
    root: CertificateDer<'static>,
    crypto_provider: Arc<CryptoProvider>,
) -> Arc<dyn rustls::client::danger::ServerCertVerifier> {
    Arc::new(Verifier::new_with_fake_root(root, crypto_provider))
}

/// Extension trait to help configure [`ClientConfig`]s with the platform verifier.
pub trait BuilderVerifierExt {
    /// Configures the `ClientConfig` with the platform verifier.
    ///
    /// ```rust
    /// use rustls::ClientConfig;
    /// use rustls_platform_verifier::BuilderVerifierExt;
    /// let config = ClientConfig::builder()
    ///     .with_platform_verifier()
    ///     .unwrap()
    ///     .with_no_client_auth();
    /// ```
    fn with_platform_verifier(
        self,
    ) -> Result<ConfigBuilder<ClientConfig, WantsClientCert>, rustls::Error>;
}

impl BuilderVerifierExt for ConfigBuilder<ClientConfig, WantsVerifier> {
    fn with_platform_verifier(
        self,
    ) -> Result<ConfigBuilder<ClientConfig, WantsClientCert>, rustls::Error> {
        let verifier = Verifier::new(self.crypto_provider().clone())?;
        Ok(self
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(verifier)))
    }
}

/// Extension trait to help build a [`ClientConfig`] with the platform verifier.
pub trait ConfigVerifierExt {
    /// Build a [`ClientConfig`] with the platform verifier and the default `CryptoProvider`.
    ///
    /// ```rust
    /// use rustls::ClientConfig;
    /// use rustls_platform_verifier::ConfigVerifierExt;
    /// let config = ClientConfig::with_platform_verifier();
    /// ```
    fn with_platform_verifier() -> Result<ClientConfig, rustls::Error>;
}

impl ConfigVerifierExt for ClientConfig {
    fn with_platform_verifier() -> Result<ClientConfig, rustls::Error> {
        Ok(ClientConfig::builder()
            .with_platform_verifier()?
            .with_no_client_auth())
    }
}
