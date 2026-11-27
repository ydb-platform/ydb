#include "tls_utils.h"

#include <util/generic/yexception.h>
#include <util/stream/file.h>
#include <util/string/builder.h>

#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/pem.h>
#include <openssl/x509.h>

#include <algorithm>
#include <ctime>
#include <limits>
#include <memory>
#include <vector>

namespace NYdb::NBS::NTlsUtils {

using NYdb::NBS::E_INVALID_STATE;
using NYdb::NBS::E_IO;
using NYdb::NBS::FACILITY_SYSTEM;
using NYdb::NBS::FormatError;
using NYdb::NBS::HasError;
using NYdb::NBS::SEVERITY_ERROR;
using NYdb::NBS::TErrorResponse;

namespace {

////////////////////////////////////////////////////////////////////////////////

using TBioPtr = std::unique_ptr<BIO, decltype(&BIO_free)>;
using TX509Ptr = std::unique_ptr<X509, decltype(&X509_free)>;
using TEvpPkeyPtr = std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)>;

////////////////////////////////////////////////////////////////////////////////

// Helper RAII-style class to clear Ssl error
struct TSslErrorQueueGuard
{
    ~TSslErrorQueueGuard()
    {
        ERR_clear_error();
    }
};

////////////////////////////////////////////////////////////////////////////////

TString GetLastOpenSslError()
{
    const ui64 error = ERR_peek_last_error();
    if (error == 0) {
        return {};
    }
    const char* lib = ERR_lib_error_string(error);
    const char* reason = ERR_reason_error_string(error);

    TStringBuilder result;
    if (lib) {
        result << lib;
    }
    if (reason) {
        if (!result.empty()) {
            result << ": ";
        }
        result << reason;
    }

    if (result.empty()) {
        // No textual description available. Try to use legacy
        // error reporting
        char buffer[256];
        ERR_error_string_n(error, buffer, sizeof(buffer));
        result << buffer;
    }

    return result;
}

TString OpenSslErrorToString(TStringBuf message)
{
    const auto details = GetLastOpenSslError();
    if (details.empty()) {
        return TString(message);
    }
    return TStringBuilder() << message << ": " << details;
}

TErrorResponse MakeOpenSslError(TStringBuf message)
{
    const ui64 error = ERR_peek_last_error();
    return {
        MAKE_SYSTEM_ERROR(ERR_GET_REASON(error)),
        OpenSslErrorToString(message)};
}

////////////////////////////////////////////////////////////////////////////////

TResultOrError<TVector<TX509Ptr>> ParsePemCertificates(TStringBuf pem)
{
    TSslErrorQueueGuard errorGuard;

    TVector<TX509Ptr> certificates;
    TBioPtr bio(
        BIO_new_mem_buf(pem.data(), static_cast<int>(pem.size())),
        BIO_free);
    if (!bio) {
        return MakeOpenSslError("Failed to allocate BIO for PEM certificates");
    }

    while (true) {
        X509* cert = PEM_read_bio_X509(bio.get(), nullptr, nullptr, nullptr);
        if (cert != nullptr) {
            certificates.emplace_back(cert, X509_free);
            continue;
        }

        const ui64 error = ERR_peek_last_error();
        if (error == 0) {
            break;
        }

        // Not an error. This way OpenSSL indicates the end of cert chain.
        if (ERR_GET_LIB(error) == ERR_LIB_PEM &&
            ERR_GET_REASON(error) == PEM_R_NO_START_LINE)
        {
            break;
        }

        return MakeOpenSslError("Failed to parse PEM certificates");
    }

    return certificates;
}

TResultOrError<TVector<TX509Ptr>> ParseNonEmptyPemCertificates(
    TStringBuf pem,
    TStringBuf description)
{
    auto result = ParsePemCertificates(pem);
    if (HasError(result.GetError())) {
        return result.GetError();
    }
    if (result.GetResult().empty()) {
        const auto message = TStringBuilder()
                             << description << " contains no certificates";
        return TErrorResponse(E_INVALID_STATE, message);
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

bool IsEmptyPair(const TCertificateFiles& certPair)
{
    return !certPair.PrivateKeyPath && !certPair.CertChainPath;
}

////////////////////////////////////////////////////////////////////////////////

TMaybe<TString> UpdateRootCa(const TRootCaPair& root, TLog& Log)
{
    if (root.RootCaPath.empty()) {
        return Nothing();
    }

    auto result = NTlsUtils::ReadAndValidateRootCertificate(root.RootCaPath);
    if (HasError(result.GetError())) {
        STORAGE_WARN(
            "Root certificate update is skipped: "
            << FormatError(result.GetError()));

        return root.RootCa.empty() ? Nothing() : TMaybe<TString>(root.RootCa);
    }

    return result.ExtractResult();
}

TResultOrError<grpc_core::PemKeyCertPairList>
ReadAndValidateIdentityCertificate(const TCertificateFiles& files)
{
    auto identityResult = NTlsUtils::ReadAndValidateIdentityPair(files);
    if (HasError(identityResult)) {
        return identityResult.GetError();
    }

    const auto& pair = identityResult.GetResult().front();
    auto validityResult =
        NTlsUtils::ValidateIdentityCertificateValidity(pair.cert_chain());
    if (HasError(validityResult)) {
        return validityResult.GetError();
    }

    return identityResult.ExtractResult();
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TResultOrError<TString> TryReadFile(const TString& path)
{
    try {
        TFileInput in(path);
        return in.ReadAll();
    } catch (const std::exception& e) {
        const auto message = TStringBuilder()
                             << "Reading certificate file " << path.Quote()
                             << " failed: " << e.what();
        return TErrorResponse(E_IO, message);
    }
}

TResultOrError<void> IsValidPemCertificate(TStringBuf pem)
{
    if (pem.empty()) {
        return TErrorResponse(E_INVALID_STATE, "PEM certificate is empty");
    }

    auto parseResult = ParseNonEmptyPemCertificates(pem, "PEM");
    if (HasError(parseResult.GetError())) {
        return parseResult.GetError();
    }
    return {};
}

TResultOrError<void> PrivateKeyAndCertificateMatch(
    TStringBuf privateKey,
    TStringBuf certChain)
{
    TSslErrorQueueGuard errorGuard;

    TBioPtr certBio(
        BIO_new_mem_buf(certChain.data(), static_cast<int>(certChain.size())),
        BIO_free);
    if (!certBio) {
        return MakeOpenSslError("Failed to allocate BIO for certificate chain");
    }
    TX509Ptr cert(
        PEM_read_bio_X509(certBio.get(), nullptr, nullptr, nullptr),
        X509_free);
    if (!cert) {
        return MakeOpenSslError("Failed to parse certificate chain PEM");
    }
    TEvpPkeyPtr publicKey(X509_get_pubkey(cert.get()), EVP_PKEY_free);
    if (!publicKey) {
        return MakeOpenSslError(
            "Failed to extract public key from certificate");
    }
    TBioPtr keyBio(
        BIO_new_mem_buf(privateKey.data(), static_cast<int>(privateKey.size())),
        BIO_free);
    if (!keyBio) {
        return MakeOpenSslError("Failed to allocate BIO for private key");
    }
    TEvpPkeyPtr privateKeyObj(
        PEM_read_bio_PrivateKey(keyBio.get(), nullptr, nullptr, nullptr),
        EVP_PKEY_free);
    if (!privateKeyObj) {
        return MakeOpenSslError("Failed to parse private key PEM");
    }
    if (EVP_PKEY_cmp(privateKeyObj.get(), publicKey.get()) != 1) {
        return TErrorResponse(
            E_INVALID_STATE,
            "Private key does not match certificate public key");
    }
    return {};
}

TResultOrError<void> ValidateIdentityCertificateValidity(
    TStringBuf certChainPem)
{
    auto identityChainResult = ParseNonEmptyPemCertificates(
        certChainPem,
        "Identity certificate chain");
    if (HasError(identityChainResult.GetError())) {
        return identityChainResult.GetError();
    }

    const auto& identityChain = identityChainResult.GetResult();
    for (size_t i = 0; i < identityChain.size(); ++i) {
        const X509* certificate = identityChain[i].get();
        const int notBeforeComparison =
            X509_cmp_current_time(X509_get0_notBefore(certificate));
        const int notAfterComparison =
            X509_cmp_current_time(X509_get0_notAfter(certificate));
        if (notBeforeComparison == 0 || notAfterComparison == 0) {
            return TErrorResponse(
                E_INVALID_STATE,
                TStringBuilder()
                    << "Failed to parse validity period for identity "
                    << "certificate #" << i);
        }
        if (notBeforeComparison > 0) {
            return TErrorResponse(
                E_INVALID_STATE,
                TStringBuilder()
                    << "Identity certificate #" << i << " is not valid yet");
        }
        if (notAfterComparison < 0) {
            return TErrorResponse(
                E_INVALID_STATE,
                TStringBuilder()
                    << "Identity certificate #" << i << " has expired");
        }
    }

    return {};
}

TResultOrError<ui64> GetCertificateNotAfterTimestampSec(TStringBuf certChainPem)
{
    TSslErrorQueueGuard errorGuard;

    auto chainResult = ParseNonEmptyPemCertificates(
        certChainPem,
        "Identity certificate chain");
    if (HasError(chainResult.GetError())) {
        return chainResult.GetError();
    }

    const auto& chain = chainResult.GetResult();
    ui64 earliestTimestamp = std::numeric_limits<ui64>::max();
    for (size_t i = 0; i < chain.size(); ++i) {
        const ASN1_TIME* notAfter = X509_get0_notAfter(chain[i].get());
        if (!notAfter) {
            return TErrorResponse(
                E_INVALID_STATE,
                TStringBuilder() << "Failed to get notAfter field for identity "
                                 << "certificate #" << i);
        }

        tm tmValue{};
        if (ASN1_TIME_to_tm(notAfter, &tmValue) != 1) {
            return MakeOpenSslError(
                TStringBuilder()
                << "Failed to parse notAfter field for identity "
                << "certificate #" << i);
        }

        const time_t timestamp = timegm(&tmValue);
        if (timestamp < 0) {
            return TErrorResponse(
                E_INVALID_STATE,
                TStringBuilder() << "Invalid notAfter timestamp for identity "
                                 << "certificate #" << i);
        }
        earliestTimestamp =
            std::min(earliestTimestamp, static_cast<ui64>(timestamp));
    }

    return earliestTimestamp;
}

TResultOrError<TString> ReadAndValidateRootCertificate(
    const TString& rootCertPath)
{
    auto pem = TryReadFile(rootCertPath);
    if (HasError(pem.GetError())) {
        return pem.GetError();
    }
    auto certValidity = IsValidPemCertificate(pem.GetResult());
    if (HasError(certValidity.GetError())) {
        return certValidity.GetError();
    }
    return pem.ExtractResult();
}

TResultOrError<grpc_core::PemKeyCertPairList> ReadAndValidateIdentityPair(
    const TCertificateFiles& files)
{
    auto privateKey = TryReadFile(files.PrivateKeyPath);
    if (HasError(privateKey.GetError())) {
        return privateKey.GetError();
    }
    auto certChain = TryReadFile(files.CertChainPath);
    if (HasError(certChain.GetError())) {
        return certChain.GetError();
    }
    auto keyMatchesCert = PrivateKeyAndCertificateMatch(
        privateKey.GetResult(),
        certChain.GetResult());
    if (HasError(keyMatchesCert.GetError())) {
        return keyMatchesCert.GetError();
    }

    grpc_core::PemKeyCertPairList result;
    result.emplace_back(privateKey.ExtractResult(), certChain.ExtractResult());
    return result;
}

TVector<TCertificatePair> LoadCertificatePairs(
    TVector<TCertificateFiles> certificates)
{
    auto prepared = PrepareCertificateFilePairs(std::move(certificates));

    TVector<TCertificatePair> result;
    result.reserve(prepared.size());
    for (auto& cert: prepared) {
        auto keyCertPair = ReadAndValidateIdentityPair(cert);
        if (HasError(keyCertPair.GetError())) {
            ythrow yexception() << keyCertPair.GetError().GetMessage();
        }

        const auto& keyCert = keyCertPair.GetResult().front();
        result.push_back({
            .Files = std::move(cert),
            .PrivateKey = TString(keyCert.private_key()),
            .CertChain = TString(keyCert.cert_chain()),
        });
    }
    return result;
}

TRootCaPair LoadRootCaPair(TString rootCaPath)
{
    if (!rootCaPath) {
        return {};
    }

    auto rootCa = ReadAndValidateRootCertificate(rootCaPath);
    if (HasError(rootCa.GetError())) {
        ythrow yexception() << rootCa.GetError().GetMessage();
    }

    return {
        .RootCaPath = std::move(rootCaPath),
        .RootCa = rootCa.ExtractResult(),
    };
}

TVector<TCertificateFiles> PrepareCertificateFilePairs(
    TVector<TCertificateFiles> certificates)
{
    TVector<TCertificateFiles> res;
    for (size_t i = 0; i < certificates.size(); ++i) {
        auto& cert = certificates[i];
        if (IsEmptyPair(cert)) {
            continue;
        }
        if (!cert.PrivateKeyPath) {
            ythrow yexception()
                << "Empty PrivateKeyPath for certificate #" << i;
        }
        if (!cert.CertChainPath) {
            ythrow yexception() << "Empty CertChainPath for certificate #" << i;
        }
        res.emplace_back(std::move(cert));
    }
    return res;
}

TCertificatesUpdateResult UpdateCertificates(
    const TVector<TCertificatePair>& certificates,
    const TRootCaPair& root,
    TLog& log)
{
    TLog& Log = log;

    TCertificatesUpdateResult updateResult;
    updateResult.Certificates.resize(certificates.size());
    updateResult.RootCa = UpdateRootCa(root, Log);

    for (size_t i = 0; i < certificates.size(); ++i) {
        const TCertificatePair& cert = certificates[i];
        auto identityResult = ReadAndValidateIdentityCertificate(cert.Files);

        if (HasError(identityResult)) {
            STORAGE_WARN(
                "Identity certificate update is skipped for "
                << cert.Files.CertChainPath.Quote() << ": "
                << FormatError(identityResult.GetError()));

            if (!cert.PrivateKey.empty() && !cert.CertChain.empty()) {
                grpc_core::PemKeyCertPairList fallback;
                fallback.emplace_back(cert.PrivateKey, cert.CertChain);
                updateResult.Certificates[i] = TCertificate{
                    .CertificatesChain = std::move(fallback),
                };
            }

            continue;
        }

        TCertificate newCert;
        newCert.CertificatesChain = identityResult.ExtractResult();

        const auto& identityPair = newCert.CertificatesChain.front();
        auto notAfterTs = NTlsUtils::GetCertificateNotAfterTimestampSec(
            identityPair.cert_chain());
        if (HasError(notAfterTs)) {
            STORAGE_WARN(
                "Unable to parse certificate notAfter date for "
                << cert.Files.CertChainPath.Quote() << ": "
                << FormatError(notAfterTs.GetError()));
        } else {
            newCert.NotValidAfter =
                TInstant::Seconds(notAfterTs.ExtractResult());
        }

        updateResult.Certificates[i] = std::move(newCert);
    }

    return updateResult;
}

}   // namespace NYdb::NBS::NTlsUtils
