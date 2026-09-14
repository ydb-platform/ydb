#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/secret/secret.h>

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/src/client/impl/internal/make_request/make.h>
#include <ydb/public/sdk/cpp/src/client/impl/internal/scheme_helpers/helpers.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <ydb/public/api/grpc/ydb_secret_v1.grpc.pb.h>
#include <ydb/public/api/protos/ydb_secret.pb.h>
#include <ydb/public/sdk/cpp/src/client/common_client/impl/client.h>

namespace NYdb::inline Dev {
namespace NSecret {

class TSecretClient::TImpl : public TClientImplCommon<TSecretClient::TImpl> {
public:
    TImpl(std::shared_ptr<TGRpcConnectionsImpl>&& connections, const TCommonClientSettings& settings)
        : TClientImplCommon(std::move(connections), settings) {}

    TAsyncDescribeSecretResult DescribeSecret(const std::string& path, const NScheme::TDescribePathSettings& settings) {
        auto request = MakeOperationRequest<::Ydb::Secret::DescribeSecretRequest>(settings);
        request.set_path(TStringType{path});

        auto promise = NThreading::NewPromise<TDescribeSecretResult>();
        auto extractor = [promise](google::protobuf::Any* any, TPlainStatus status) mutable {
            ::Ydb::Secret::DescribeSecretResult result;
            if (any) {
                any->UnpackTo(&result);
            }
            NScheme::TSchemeEntry entry(result.self());
            promise.SetValue(TDescribeSecretResult(
                TStatus(std::move(status)),
                std::move(entry),
                static_cast<uint64_t>(result.version())));
        };

        Connections_->RunDeferred<::Ydb::Secret::V1::SecretService, ::Ydb::Secret::DescribeSecretRequest, ::Ydb::Secret::DescribeSecretResponse>(
            std::move(request),
            extractor,
            &::Ydb::Secret::V1::SecretService::Stub::AsyncDescribeSecret,
            DbDriverState_,
            INITIAL_DEFERRED_CALL_DELAY,
            TRpcRequestSettings::Make(settings));

        return promise.GetFuture();
    }
};

TDescribeSecretResult::TDescribeSecretResult(TStatus&& status, NScheme::TSchemeEntry&& entry, uint64_t version)
    : TStatus(std::move(status))
    , Entry_(std::move(entry))
    , Version_(version)
{}

const NScheme::TSchemeEntry& TDescribeSecretResult::GetEntry() const {
    CheckStatusOk("TDescribeSecretResult::GetEntry");
    return Entry_;
}

const std::string& TDescribeSecretResult::GetName() const {
    CheckStatusOk("TDescribeSecretResult::GetName");
    return Entry_.Name;
}

uint64_t TDescribeSecretResult::GetVersion() const {
    CheckStatusOk("TDescribeSecretResult::GetVersion");
    return Version_;
}

void TDescribeSecretResult::Out(IOutputStream& out) const {
    if (IsSuccess()) {
        Entry_.Out(out);
    } else {
        TStatus::Out(out);
    }
}

void TDescribeSecretResult::SerializeTo(::Ydb::Scheme::Entry* proto) const {
    CheckStatusOk("TDescribeSecretResult::SerializeTo");
    NScheme::SchemeEntryToProto(Entry_, proto);
}

TSecretClient::TSecretClient(const TDriver& driver, const TCommonClientSettings& settings)
    : Impl_(new TImpl(CreateInternalInterface(driver), settings))
{}

TAsyncDescribeSecretResult TSecretClient::DescribeSecret(const std::string& path, const NScheme::TDescribePathSettings& settings) {
    return Impl_->DescribeSecret(path, settings);
}

} // namespace NSecret
} // namespace NYdb
