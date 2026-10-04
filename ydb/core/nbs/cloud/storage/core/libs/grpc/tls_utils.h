#pragma once

#include "tls_certificate_provider.h"

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>
#include <ydb/core/nbs/cloud/storage/core/libs/diagnostics/logging.h>

#include <util/generic/strbuf.h>

#include <src/core/lib/security/credentials/tls/grpc_tls_certificate_provider.h>

namespace NYdb::NBS::NTlsUtils {

using NYdb::NBS::TResultOrError;

////////////////////////////////////////////////////////////////////////////////

struct TCertificatePair
{
    TCertificateFiles Files;
    TString PrivateKey;
    TString CertChain;
};

struct TRootCaPair
{
    TString RootCaPath;
    TString RootCa;
};

struct TCertificate
{
    grpc_core::PemKeyCertPairList CertificatesChain;
    TInstant NotValidAfter;
};

struct TCertificatesUpdateResult
{
    TVector<TMaybe<TCertificate>> Certificates;
    TMaybe<TString> RootCa;
};

////////////////////////////////////////////////////////////////////////////////

TResultOrError<TString> TryReadFile(const TString& path);

TResultOrError<void> IsValidPemCertificate(TStringBuf pem);

TResultOrError<void> PrivateKeyAndCertificateMatch(
    TStringBuf privateKey,
    TStringBuf certChain);

TResultOrError<void> ValidateIdentityCertificateValidity(
    TStringBuf certChainPem);

TResultOrError<ui64> GetCertificateNotAfterTimestampSec(
    TStringBuf certChainPem);

TResultOrError<TString> ReadAndValidateRootCertificate(
    const TString& rootCertPath);

TResultOrError<grpc_core::PemKeyCertPairList> ReadAndValidateIdentityPair(
    const TCertificateFiles& files);

TVector<TCertificateFiles> PrepareCertificateFilePairs(
    TVector<TCertificateFiles> certificates);

TVector<TCertificatePair> LoadCertificatePairs(
    TVector<TCertificateFiles> certificates);

TRootCaPair LoadRootCaPair(TString rootCaPath);

TCertificatesUpdateResult UpdateCertificates(
    const TVector<TCertificatePair>& certificates,
    const TRootCaPair& root,
    TLog& log);

}   // namespace NYdb::NBS::NTlsUtils
