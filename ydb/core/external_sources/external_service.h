#pragma once

#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <util/generic/string.h>

namespace NKikimr::NExternalSource {

// Public metadata only. The referenced secret is resolved by the query executor.
struct TServiceConnectionSettings {
    TString Protocol;
    TString Endpoint;
    bool UseTls = true;
    TString CaCertificate;
    TString SecretReference;
    TString AuthProvider;
    TString AuthParameters;
    ui32 TvmSourceId = 0;
    ui32 TvmDestinationId = 0;

    TString Serialize() const;
    static TServiceConnectionSettings Deserialize(const TString& value);
    void Validate() const;
};

TServiceConnectionSettings ParseServiceConnection(const NKikimrSchemeOp::TExternalDataSourceDescription& description);

}   // namespace NKikimr::NExternalSource
