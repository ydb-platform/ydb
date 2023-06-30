#pragma once

#include <library/cpp/threading/future/future.h>

#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>


namespace NYql {

struct TStatUploadOptions {
    TString ProviderName;
    TString SessionId;
    ui32 PublicId;

    TString Cluster;
    TString Table;
    TString Scale;
    TVector<TString> ReplaceMask;

    TString YtServer;
    TString YtTable;
    TString YtTx;
    TString YtToken;
    TMaybe<TString> YtPool;
};


class IStatUploader : public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<IStatUploader>;

    virtual NThreading::TFuture<void> Upload(TStatUploadOptions&& options) = 0;

    virtual ~IStatUploader() = default;
};

} // namespace NYql
