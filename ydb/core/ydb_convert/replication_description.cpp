#include "replication_description.h"
#include "ydb_convert.h"

#include <ydb/core/protos/replication.pb.h>
#include <ydb/public/api/protos/draft/ydb_replication.pb.h>

#include <google/protobuf/util/time_util.h>

namespace NKikimr {

namespace {

TString BuildConnectionString(const NKikimrReplication::TConnectionParams& params) {
    return TStringBuilder()
        << (params.GetEnableSsl() ? "grpcs://" : "grpc://")
        << params.GetEndpoint()
        << "/?database=" << params.GetDatabase();
}

void ConvertStaticCredentials(
    const NKikimrReplication::TStaticCredentials& from,
    Ydb::Replication::ConnectionParams::StaticCredentials& to)
{
    to.set_user(from.GetUser());
    to.set_password_secret_name(from.GetPasswordSecretName());
}

void ConvertOAuth(
    const NKikimrReplication::TOAuthToken& from,
    Ydb::Replication::ConnectionParams::OAuth& to)
{
    to.set_token_secret_name(from.GetTokenSecretName());
}

void ConvertConnectionParams(
    const NKikimrReplication::TConnectionParams& from,
    Ydb::Replication::ConnectionParams& to)
{
    to.set_endpoint(from.GetEndpoint());
    to.set_database(from.GetDatabase());
    to.set_enable_ssl(from.GetEnableSsl());
    to.set_connection_string(BuildConnectionString(from));

    switch (from.GetCredentialsCase()) {
    case NKikimrReplication::TConnectionParams::kStaticCredentials:
        return ConvertStaticCredentials(from.GetStaticCredentials(), *to.mutable_static_credentials());
    case NKikimrReplication::TConnectionParams::kOAuthToken:
        return ConvertOAuth(from.GetOAuthToken(), *to.mutable_oauth());
    default:
        break;
    }
}

void ConvertRowConsistencySettings(
    const NKikimrReplication::TConsistencySettings::TRowConsistency&,
    Ydb::Replication::ConsistencyLevelRow&)
{
    // nop
}

void ConvertGlobalConsistencySettings(
    const NKikimrReplication::TConsistencySettings::TGlobalConsistency& from,
    Ydb::Replication::ConsistencyLevelGlobal& to)
{
    *to.mutable_commit_interval() = google::protobuf::util::TimeUtil::MillisecondsToDuration(
        from.GetCommitIntervalMilliSeconds());
}

void ConvertConsistencySettings(
    const NKikimrReplication::TConsistencySettings& from,
    Ydb::Replication::DescribeReplicationResult& to)
{
    switch (from.GetLevelCase()) {
    case NKikimrReplication::TConsistencySettings::kRow:
        return ConvertRowConsistencySettings(from.GetRow(), *to.mutable_row_consistency());
    case NKikimrReplication::TConsistencySettings::kGlobal:
        return ConvertGlobalConsistencySettings(from.GetGlobal(), *to.mutable_global_consistency());
    default:
        break;
    }
}

void ConvertItem(
    const NKikimrReplication::TReplicationConfig::TTargetSpecific::TTarget& from,
    Ydb::Replication::DescribeReplicationResult::Item& to)
{
    to.set_id(from.GetId());
    to.set_source_path(from.GetSrcPath());
    to.set_destination_path(from.GetDstPath());
    if (from.HasSrcStreamName()) {
        to.set_source_changefeed_name(from.GetSrcStreamName());
    }
    if (from.HasLagMilliSeconds()) {
        *to.mutable_stats()->mutable_lag() = google::protobuf::util::TimeUtil::MillisecondsToDuration(
            from.GetLagMilliSeconds());
    }
    if (from.HasInitialScanProgress()) {
        to.mutable_stats()->set_initial_scan_progress(from.GetInitialScanProgress());
    }
}

void ConvertStats(
    const NKikimrReplication::TReplicationState& from,
    Ydb::Replication::DescribeReplicationResult& to)
{
    if (from.GetStandBy().HasLagMilliSeconds()) {
        *to.mutable_running()->mutable_stats()->mutable_lag() = google::protobuf::util::TimeUtil::MillisecondsToDuration(
            from.GetStandBy().GetLagMilliSeconds());
    }
    if (from.GetStandBy().HasInitialScanProgress()) {
        to.mutable_running()->mutable_stats()->set_initial_scan_progress(from.GetStandBy().GetInitialScanProgress());
    }
}

void ConvertStats(
    const NKikimrReplication::TReplicationState&,
    Ydb::Replication::DescribeTransferResult&)
{
    // nop
}

void ConvertStats(
    const NKikimrReplication::TReplicationStats& from,
    Ydb::Replication::DescribeTransferResult& to)
{
    if (from.HasTransfer()) {
        to.mutable_stats()->CopyFrom(from.GetTransfer());
    }
}

template<typename T>
void ConvertState(const NKikimrReplication::TReplicationState& from, T& to) {
    switch (from.GetStateCase()) {
    case NKikimrReplication::TReplicationState::kStandBy:
        to.mutable_running();
        ConvertStats(from, to);
        break;
    case NKikimrReplication::TReplicationState::kError:
        *to.mutable_error()->mutable_issues() = from.GetError().GetIssues();
        break;
    case NKikimrReplication::TReplicationState::kDone:
        to.mutable_done();
        break;
    case NKikimrReplication::TReplicationState::kPaused:
        to.mutable_paused();
        break;
    default:
        break;
    }
}

template <NKikimrReplication::TReplicationConfig::TargetCase CorrectTargetCase>
bool CheckConfig(
    const NKikimrReplication::TReplicationConfig& config,
    Ydb::StatusIds_StatusCode& status,
    TString& error)
{
    switch (config.GetTargetCase()) {
        case CorrectTargetCase:
            return true;
        default:
            error = TStringBuilder() << "wrong config: "
                << CorrectTargetCase << " expected, "
                << config.GetTargetCase() << " provided";
            break;
    }

    status = Ydb::StatusIds::INTERNAL_ERROR;
    return false;
}

constexpr auto CheckReplicationConfig = CheckConfig<NKikimrReplication::TReplicationConfig::TargetCase::kSpecific>;

} // anonymous namespace

void FillReplicationDescription(
    Ydb::Replication::DescribeReplicationResult& out,
    const NKikimrReplication::TEvDescribeReplicationResult& inDesc)
{
    ConvertConnectionParams(inDesc.GetConnectionParams(), *out.mutable_connection_params());
    ConvertConsistencySettings(inDesc.GetConsistencySettings(), out);
    ConvertState(inDesc.GetState(), out);

    for (const auto& target : inDesc.GetTargets()) {
        ConvertItem(target, *out.add_items());
    }
}

bool FillReplicationDescription(
    Ydb::Replication::DescribeReplicationResult& out,
    const NKikimrSchemeOp::TReplicationDescription& inDesc,
    const NKikimrSchemeOp::TDirEntry& inDirEntry,
    Ydb::StatusIds_StatusCode& status,
    TString& error)
{
    const auto& config = inDesc.GetConfig();
    if (!CheckReplicationConfig(config, status, error)) {
        return false;
    }

    ConvertDirectoryEntry(inDirEntry, out.mutable_self(), true);
    ConvertConnectionParams(config.GetSrcConnectionParams(), *out.mutable_connection_params());
    ConvertConsistencySettings(config.GetConsistencySettings(), out);
    ConvertState(inDesc.GetState(), out);

    for (const auto& target : config.GetSpecific().GetTargets()) {
        ConvertItem(target, *out.add_items());
    }

    return true;
}

namespace {

void ConvertTransferSpecific(
    const NKikimrReplication::TReplicationConfig_TTransferSpecific& from,
    Ydb::Replication::DescribeTransferResult& to)
{
    to.set_source_path(from.GetTarget().GetSrcPath());
    to.set_destination_path(from.GetTarget().GetDstPath());
    to.set_consumer_name(from.GetTarget().GetConsumerName());
    to.set_transformation_lambda(from.GetTarget().GetTransformLambda());
    to.mutable_batch_settings()->set_size_bytes(from.GetBatching().GetBatchSizeBytes());
    to.mutable_batch_settings()->mutable_flush_interval()->set_seconds(from.GetBatching().GetFlushIntervalMilliSeconds() / 1000);
}

constexpr auto CheckTransferConfig = CheckConfig<NKikimrReplication::TReplicationConfig::TargetCase::kTransferSpecific>;

} // anonymous namespace

void FillTransferDescription(
    Ydb::Replication::DescribeTransferResult& out,
    const NKikimrReplication::TEvDescribeReplicationResult& inDesc)
{
    ConvertConnectionParams(inDesc.GetConnectionParams(), *out.mutable_connection_params());
    ConvertState(inDesc.GetState(), out);
    ConvertTransferSpecific(inDesc.GetTransferSpecific(), out);
    ConvertStats(inDesc.GetStats(), out);
}

bool FillTransferDescription(
    Ydb::Replication::DescribeTransferResult& out,
    const NKikimrSchemeOp::TReplicationDescription& inDesc,
    const NKikimrSchemeOp::TDirEntry& inDirEntry,
    Ydb::StatusIds_StatusCode& status,
    TString& error)
{
    const auto& config = inDesc.GetConfig();
    if (!CheckTransferConfig(config, status, error)) {
        return false;
    }

    ConvertDirectoryEntry(inDirEntry, out.mutable_self(), true);
    ConvertConnectionParams(config.GetSrcConnectionParams(), *out.mutable_connection_params());
    ConvertState(inDesc.GetState(), out);
    ConvertTransferSpecific(config.GetTransferSpecific(), out);

    return true;
}

} // namespace NKikimr
