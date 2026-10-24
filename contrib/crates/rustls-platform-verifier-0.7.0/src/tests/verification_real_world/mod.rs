//! Certificate verification tests that use real-world certificates and the
//! "real" (non-mock) Rustls configuration returned.
//!
//! # Repeatability and Self-containedness
//!
//! These tests are written to reduce the time-bomb nature of testing with
//! real certificates, which expire and/or can be revoked over time. For
//! example, rather than connecting to the TLS server over the network,
//! these tests operate on a locally-cached copy of the certificates
//! downloaded at a point in time. However, there are some inherent
//! limitations of what we can do when using real-world certificates. We
//! accept that the benefit of having these tests outweigh this downside.
//! If we encounter cases where these tests are flaky we'll spend additional
//! effort
//!
//! * If these certificates are ever revoked then it is possible that, even if
//!   with the measures mentioned in the next paragraphs, the operating system
//!   might learn of the revocation externally and cause the tests to fail.
//!
//! * Some operating systems, Windows in particular, download the set of
//!   trusted roots dynamically as-needed. If there is a failure during that
//!   fetching then the trust anchors for these certificates might not be
//!   trusted by the operating system's root store.
//!
//! XXX: These tests should be using a stapled OCSP responses so that the
//! (operating-system-based) verifier doesn't try to fetch an OCSP
//! response or CRL certificate. However, until we can fix the validation
//! at a specific point in time, we can't do this, as the OCSP responses
//! will generally expire within a matter of days of being produced. Also,
//! we'd need to upgrade to a version of Rustls that supports passing in
//! stapled OCSP responses for each certificate in the chain. Most certificate
//! verifiers that do fetching of OCSP responses will "fail open"; that is, if
//! a networking error causes the fetch of the OCSP response to fail, then
//! they will continue roughly as though they received a "Good" response.
//! Thus we don't expect these tests to be flaky w.r.t. that, except for
//! potentially poor performance.

use rustls::client::danger::ServerCertVerifier;
use rustls::pki_types;
#[cfg(not(any(target_vendor = "apple", windows)))]
use rustls::pki_types::{DnsName, ServerName};
use rustls::{CertificateError, Error as TlsError};

use super::TestCase;
use crate::tests::{assert_cert_error_eq, test_provider, verification_time};
use crate::Verifier;

// This is the certificate chain presented by one server for
// `aws.amazon.com` when this test was updated 2025-08-13.
//
// Use this to template view the certificate using OpenSSL:
// ```sh
// openssl x509 -inform der -text -in aws_amazon_com_valid_1.crt | less
// ```
//
// You can update these cert files with `examples/update-certs.rs`
const VALID_AWS_AMAZON_COM_CHAIN: &[&[u8]] = &[
    include_bytes!("aws_amazon_com_valid_1.crt"),
    include_bytes!("aws_amazon_com_valid_2.crt"),
    include_bytes!("aws_amazon_com_valid_3.crt"),
    // XXX: This certificate is included for testing in environments that might need
    // a cross-signed root certificate instead of the just the server-provided one.
    include_bytes!("aws_amazon_com_valid_4.crt"),
];

/// Returns a list of names valid for [VALID_AWS_AMAZON_COM_CHAIN], in a format
/// expected by `CertificateError::NotValidForContext`.
#[cfg(not(any(target_vendor = "apple", windows)))]
fn valid_aws_chain_names() -> Vec<String> {
    const VALID_AWS_NAMES: &[&str] = &[
        "aws.amazon.com",
        "aws-us-west-2.amazon.com",
        "www.aws.amazon.com",
        "1.aws-lbr.amazonaws.com",
        "amazonaws-china.com",
        "www.amazonaws-china.com",
        "aws-us-east-1.amazon.com",
    ];

    VALID_AWS_NAMES
        .iter()
        .copied()
        .map(|name| format!("DnsName(\"{name}\")"))
        .collect()
}

const AWS_AMAZON_COM: &str = "aws.amazon.com";

// Domain names for which `VALID_AWS_AMAZON_COM_CHAIN` isn't valid.
const VALID_UNRELATED_DOMAIN: &str = "my.1password.com";
const VALID_UNRELATED_SUBDOMAIN: &str = "www.amazon.com";

const LETSENCRYPT_ORG: &str = "letsencrypt.org";

const VALID_LETSENCRYPT_ORG_CHAIN: &[&[u8]] = &[
    include_bytes!("letsencrypt_org_valid_1.crt"),
    include_bytes!("letsencrypt_org_valid_2.crt"),
];

macro_rules! real_world_test_cases {
    { $( $name:ident => $test_case:expr ),+ , } => {
        real_world_test_cases!(@ $($name => $test_case),+,);

        #[cfg(test)]
        mod tests {
            $(
                #[test]
                pub fn $name() {
                    super::$name()
                }
            )+

        }

        #[cfg(feature = "ffi-testing")]
        pub static ALL_TEST_CASES: &'static [fn()] = &[
            $($name),+
        ];
    };

    {@ $( $name:ident => $test_case:expr ),+ , } => {
        $(
            pub(super) fn $name() {
                real_world_test(&$test_case);
            }
        )+
    }
}

macro_rules! no_error {
    () => {
        None::<std::convert::Infallible>
    };
}

fn real_world_test<E: std::error::Error>(test_case: &TestCase<E>) {
    log::info!(
        "verifying ref ID {:?} expected {:?}",
        test_case.reference_id,
        test_case.expected_result
    );

    let crypto_provider = test_provider();

    // On BSD systems openssl-probe fails to find the system CA bundle,
    // so we must provide extra roots from webpki-root-cert.
    #[cfg(target_os = "freebsd")]
    let verifier = Verifier::new_with_extra_roots(
        webpki_root_certs::TLS_SERVER_ROOT_CERTS.iter().cloned(),
        crypto_provider,
    )
    .unwrap();

    #[cfg(not(target_os = "freebsd"))]
    let verifier = Verifier::new(crypto_provider).unwrap();

    let mut chain = test_case
        .chain
        .iter()
        .map(|bytes| pki_types::CertificateDer::from(*bytes));

    let end_entity_cert = chain.next().unwrap();
    let intermediates: Vec<pki_types::CertificateDer<'_>> = chain.collect();

    let server_name = pki_types::ServerName::try_from(test_case.reference_id).unwrap();

    let stapled_ocsp = test_case.stapled_ocsp.unwrap_or(&[]);

    let result = verifier
        .verify_server_cert(
            &end_entity_cert,
            &intermediates,
            &server_name,
            stapled_ocsp,
            test_case.verification_time,
        )
        .map(|_| ());

    assert_cert_error_eq(
        &result.map(|_| ()),
        &test_case.expected_result,
        None::<&std::convert::Infallible>,
    );
    // TODO: get into specifics of errors returned when it fails.
}

// Prefer to staple the OCSP response for the end-entity certificate for
// performance and repeatability.
real_world_test_cases! {
    // The certificate is valid for *.aws.amazon.com.
    aws_amazon_com_valid => TestCase {
        reference_id: AWS_AMAZON_COM,
        chain: VALID_AWS_AMAZON_COM_CHAIN,
        stapled_ocsp: None,
        verification_time: verification_time(),
        expected_result: Ok(()),
        other_error: no_error!(),
    },
    // Same as above but without stapled OCSP.
    aws_amazon_com_valid_no_stapled => TestCase {
        reference_id: AWS_AMAZON_COM,
        chain: VALID_AWS_AMAZON_COM_CHAIN,
        stapled_ocsp: None,
        verification_time: verification_time(),
        expected_result: Ok(()),
        other_error: no_error!(),
    },
    // Valid also for www.amazon.amazon.com (extra subdomain).
    _aws_amazon_com_valid => TestCase {
        reference_id: "www.aws.amazon.com",
        chain: VALID_AWS_AMAZON_COM_CHAIN,
        stapled_ocsp: None,
        verification_time: verification_time(),
        expected_result: Ok(()),
        other_error: no_error!(),
    },
    // The certificate isn't valid for an unrelated subdomain.
    unrelated_domain_invalid => TestCase {
        reference_id: VALID_UNRELATED_SUBDOMAIN,
        chain: VALID_AWS_AMAZON_COM_CHAIN,
        stapled_ocsp: None,
        verification_time: verification_time(),
        #[cfg(not(any(target_vendor = "apple", windows)))]
        expected_result: Err(TlsError::InvalidCertificate(CertificateError::NotValidForNameContext {
            expected: ServerName::DnsName(DnsName::try_from(VALID_UNRELATED_SUBDOMAIN).unwrap()),
            presented: valid_aws_chain_names(),
        })),
        #[cfg(any(target_vendor = "apple", windows))]
        expected_result: Err(TlsError::InvalidCertificate(CertificateError::NotValidForName)),
        other_error: no_error!(),
    },
    // The certificate chain for the unrelated domain is not valid for
    // my.1password.com.
    unrelated_chain_not_valid_for_my_1password_com => TestCase {
        reference_id: VALID_UNRELATED_DOMAIN,
        chain: VALID_AWS_AMAZON_COM_CHAIN,
        stapled_ocsp: None,
        verification_time: verification_time(),
        #[cfg(not(any(target_vendor = "apple", windows)))]
        expected_result: Err(TlsError::InvalidCertificate(CertificateError::NotValidForNameContext {
            expected: ServerName::DnsName(DnsName::try_from(VALID_UNRELATED_DOMAIN).unwrap()),
            presented: valid_aws_chain_names(),
        })),
        #[cfg(any(target_vendor = "apple", windows))]
        expected_result: Err(TlsError::InvalidCertificate(CertificateError::NotValidForName)),
        other_error: no_error!(),
    },
    letsencrypt => TestCase {
        reference_id: LETSENCRYPT_ORG,
        chain: VALID_LETSENCRYPT_ORG_CHAIN,
        stapled_ocsp: None,
        verification_time: verification_time(),
        expected_result: Ok(()),
        other_error: no_error!(),
    },

    // OCSP stapling works.
    //
    // XXX: This test is commented-out because it is a time-bomb due to the
    // short lifetime of the OCSP responses for the certificate.
    //
    // TODO: If/when we can validate a certificate for a specific point in time
    // during a test, re-enable this and have it test the certificate validity
    // at a point in time where the OCSP response is valid.
    //
    // revoked_badssl_com_stapled => TestCase {
    //     reference_id: "revoked.badssl.com",
    //     chain: &[
    //         include_bytes!("revoked_badssl_com_1.crt"),
    //         include_bytes!("revoked_badssl_com_2.crt"),
    //         ],
    //     stapled_ocsp: Some(include_bytes!("revoked_badssl_com_1.ocsp")),
    //     // XXX: We only do OCSP stapling on Windows.
    //     valid: !cfg!(windows),
    // },
}
