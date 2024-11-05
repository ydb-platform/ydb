#include "ydb_replication.h"

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/make_request/make.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <ydb/library/yql/public/issue/yql_issue.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>
#include <ydb/public/api/grpc/draft/ydb_replication_v1.grpc.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_common_client/impl/client.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

#include <google/protobuf/util/time_util.h>
#include <google/protobuf/repeated_field.h>

namespace NYdb {
namespace NReplication {

TConnectionParams::TConnectionParams(const Ydb::Replication::ConnectionParams& params) {
    DiscoveryEndpoint(params.endpoint());
    Database(params.database());
    SslCredentials(params.enable_ssl());

    switch (params.credentials_case()) {
    case Ydb::Replication::ConnectionParams::kStaticCredentials:
        Credentials_ = TStaticCredentials{
            .User = params.static_credentials().user(),
            .PasswordSecretName = params.static_credentials().password_secret_name(),
        };
        break;

    case Ydb::Replication::ConnectionParams::kOauth:
        Credentials_ = TOAuthCredentials{
            .TokenSecretName = params.oauth().token_secret_name(),
        };
        break;

    default:
        break;
    }
}

const TString& TConnectionParams::GetDiscoveryEndpoint() const {
    return *DiscoveryEndpoint_;
}

const TString& TConnectionParams::GetDatabase() const {
    return *Database_;
}

bool TConnectionParams::GetEnableSsl() const {
    return SslCredentials_->IsEnabled;
}

TConnectionParams::ECredentials TConnectionParams::GetCredentials() const {
    return static_cast<ECredentials>(Credentials_.index());
}

const TStaticCredentials& TConnectionParams::GetStaticCredentials() const {
    return std::get<TStaticCredentials>(Credentials_);
}

const TOAuthCredentials& TConnectionParams::GetOAuthCredentials() const {
    return std::get<TOAuthCredentials>(Credentials_);
}

static TDuration DurationToDuration(const google::protobuf::Duration& value) {
    return TDuration::MilliSeconds(google::protobuf::util::TimeUtil::DurationToMilliseconds(value));
}

TStats::TStats(const Ydb::Replication::DescribeReplicationResult_Stats& stats)
    : Lag_(stats.has_lag() ? std::make_optional(DurationToDuration(stats.lag())) : std::nullopt)
    , InitialScanProgress_(stats.has_initial_scan_progress() ? std::make_optional(stats.initial_scan_progress()) : std::nullopt)
{
}

const std::optional<TDuration>& TStats::GetLag() const {
    return Lag_;
}

const std::optional<float>& TStats::GetInitialScanProgress() const {
    return InitialScanProgress_;
}

TRunningState::TRunningState(const TStats& stats)
    : Stats_(stats)
{
}

const TStats& TRunningState::GetStats() const {
    return Stats_;
}

class TErrorState::TImpl {
public:
    NYql::TIssues Issues;

    explicit TImpl(NYql::TIssues&& issues)
        : Issues(std::move(issues))
    {
    }
};

TErrorState::TErrorState(NYql::TIssues&& issues)
    : Impl_(std::make_shared<TImpl>(std::move(issues)))
{
}

const NYql::TIssues& TErrorState::GetIssues() const {
    return Impl_->Issues;
}

template <typename T>
NYql::TIssues IssuesFromMessage(const ::google::protobuf::RepeatedPtrField<T>& message) {
    NYql::TIssues issues;
    NYql::IssuesFromMessage<T>(message, issues);
    return issues;
}

TReplicationDescription::TReplicationDescription(const Ydb::Replication::DescribeReplicationResult& desc)
    : ConnectionParams_(desc.connection_params())
{
    Items_.reserve(desc.items_size());
    for (const auto& item : desc.items()) {
        Items_.push_back(TItem{
            .Id = item.id(),
            .SrcPath = item.source_path(),
            .DstPath = item.destination_path(),
            .Stats = TStats(item.stats()),
            .SrcChangefeedName = item.has_source_changefeed_name()
                ? std::make_optional(item.source_changefeed_name()) : std::nullopt,
        });
    }

    switch (desc.state_case()) {
    case Ydb::Replication::DescribeReplicationResult::kRunning:
        State_ = TRunningState(desc.running().stats());
        break;

    case Ydb::Replication::DescribeReplicationResult::kError:
        State_ = TErrorState(IssuesFromMessage(desc.error().issues()));
        break;

    case Ydb::Replication::DescribeReplicationResult::kDone:
        State_ = TDoneState();
        break;

    default:
        break;
    }
}

const TConnectionParams& TReplicationDescription::GetConnectionParams() const {
    return ConnectionParams_;
}

const TVector<TReplicationDescription::TItem> TReplicationDescription::GetItems() const {
    return Items_;
}

TReplicationDescription::EState TReplicationDescription::GetState() const {
    return static_cast<EState>(State_.index());
}

const TRunningState& TReplicationDescription::GetRunningState() const {
    return std::get<TRunningState>(State_);
}

const TErrorState& TReplicationDescription::GetErrorState() const {
    return std::get<TErrorState>(State_);
}

const TDoneState& TReplicationDescription::GetDoneState() const {
    return std::get<TDoneState>(State_);
}

TDescribeReplicationResult::TDescribeReplicationResult(TStatus&& status, Ydb::Replication::DescribeReplicationResult&& desc)
    : NScheme::TDescribePathResult(std::move(status), desc.self())
    , ReplicationDescription_(desc)
    , Proto_(std::make_unique<Ydb::Replication::DescribeReplicationResult>())
{
    *Proto_ = std::move(desc);
}

const TReplicationDescription& TDescribeReplicationResult::GetReplicationDescription() const {
    return ReplicationDescription_;
}

const Ydb::Replication::DescribeReplicationResult& TDescribeReplicationResult::GetProto() const {
    return *Proto_;
}

class TReplicationClient::TImpl: public TClientImplCommon<TReplicationClient::TImpl> {
public:
    TImpl(std::shared_ptr<TGRpcConnectionsImpl>&& connections, const TCommonClientSettings& settings)
        : TClientImplCommon(std::move(connections), settings)
    {
    }

    TAsyncDescribeReplicationResult DescribeReplication(const TString& path, const TDescribeReplicationSettings& settings) {
        using namespace Ydb::Replication;

        auto request = MakeOperationRequest<DescribeReplicationRequest>(settings);
        request.set_path(path);
        request.set_include_stats(settings.IncludeStats_);

        auto promise = NThreading::NewPromise<TDescribeReplicationResult>();

        auto extractor = [promise]
            (google::protobuf::Any* any, TPlainStatus status) mutable {
                DescribeReplicationResult result;
                if (any) {
                    any->UnpackTo(&result);
                }

                TDescribeReplicationResult val(TStatus(std::move(status)), std::move(result));
                promise.SetValue(std::move(val));
            };

        Connections_->RunDeferred<V1::ReplicationService, DescribeReplicationRequest, DescribeReplicationResponse>(
            std::move(request),
            extractor,
            &V1::ReplicationService::Stub::AsyncDescribeReplication,
            DbDriverState_,
            INITIAL_DEFERRED_CALL_DELAY,
            TRpcRequestSettings::Make(settings));

        return promise.GetFuture();
    }

};

TReplicationClient::TReplicationClient(const TDriver& driver, const TCommonClientSettings& settings)
    : Impl_(std::make_shared<TImpl>(CreateInternalInterface(driver), settings))
{
}

TAsyncDescribeReplicationResult TReplicationClient::DescribeReplication(const TString& path, const TDescribeReplicationSettings& settings) {
    return Impl_->DescribeReplication(path, settings);
}

} // NReplication

const Ydb::Replication::DescribeReplicationResult& TProtoAccessor::GetProto(const NReplication::TDescribeReplicationResult& result) {
    return result.GetProto();
}

} // NYdb
