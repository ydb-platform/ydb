#include "common.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/auth.h>
#include <ydb/core/base/path.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/services/udf_store/metadata_subscription/storage_paths.h>
#include <ydb/services/metadata/request/request_actor_cb.h>

#include <ydb/library/aclib/aclib.h>

#include <util/string/builder.h>

#include <google/protobuf/timestamp.pb.h>

namespace NKikimr::NUdfApi {

using NUdfStore::ECompileStatus;
using NUdfStore::EUdfType;

namespace {

void FillTimestamp(TInstant value, google::protobuf::Timestamp& proto) {
    if (!value) {
        return;
    }
    proto.set_seconds(static_cast<i64>(value.Seconds()));
    proto.set_nanos(static_cast<i32>(value.MicroSecondsOfSecond() * 1000));
}

} // namespace

bool IsWasmUdfEnabled() {
    const auto& config = AppData()->UdfStoreConfig;
    return config.GetEnabled() && config.GetEnableWasmUdf();
}

void ExecuteYqlAsSystem(
    const NActors::TActorIdentity& replyTo,
    const TString& yql,
    bool readOnly,
    const std::function<void(Ydb::Table::ExecuteDataQueryRequest&)>& fillParams)
{
    auto request = NMetadata::NRequest::TDialogYQLRequest::TRequest();
    request.mutable_query()->set_yql_text(yql);
    request.mutable_query_cache_policy()->set_keep_in_cache(true);
    if (readOnly) {
        request.mutable_tx_control()->mutable_begin_tx()->mutable_snapshot_read_only();
    } else {
        request.mutable_tx_control()->mutable_begin_tx()->mutable_serializable_read_write();
        request.mutable_tx_control()->set_commit_tx(true);
    }
    fillParams(request);

    auto controller =
        std::make_shared<NMetadata::NRequest::TNaiveExternalController<NMetadata::NRequest::TDialogYQLRequest>>(replyTo);
    NMetadata::NRequest::TYQLRequestExecutor::Execute(
        std::move(request),
        NACLib::TUserToken("metadata@system", {}),
        controller);
}

bool IsDatabaseServedHere(const TString& databaseName, TString& error) {
    if (databaseName.empty()) {
        // No database header at all: the caller gets the tenant of the node it
        // reached, which is the only store this node has anyway.
        return true;
    }
    const TString requested = CanonizePath(databaseName);
    const TString served = CanonizePath(AppData()->TenantName);
    if (requested == served) {
        return true;
    }
    error = TStringBuilder()
        << "database '" << databaseName << "' is not served by this node, which serves '" << served
        << "'; the UDF store of a database can only be reached through its own endpoint";
    return false;
}

TEvTxProxySchemeCache::TEvNavigateKeySet* MakeDatabaseOwnerRequest(const TString& databaseName) {
    auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
    request->DatabaseName = databaseName;
    auto& entry = request->ResultSet.emplace_back();
    entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
    entry.Path = SplitPath(databaseName);
    return new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release());
}

bool ParseDatabaseOwner(const NSchemeCache::TSchemeCacheNavigate& response, TString& owner) {
    if (response.ResultSet.size() != 1 || response.ErrorCount > 0) {
        return false;
    }
    const auto& entry = response.ResultSet.front();
    if (entry.Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok || !entry.Self) {
        return false;
    }
    owner = entry.Self->Info.GetOwner();
    return true;
}

bool IsUdfStoreAdministrator(const NACLib::TUserToken* userToken, const TString& databaseOwner) {
    if (IsAdministrator(AppData(), userToken)) {
        return true;
    }
    return AppData()->FeatureFlags.GetEnableDatabaseAdmin()
        && IsDatabaseAdministrator(userToken, databaseOwner);
}

bool CanDecideWithoutDatabaseOwner(const NACLib::TUserToken* userToken) {
    return IsAdministrator(AppData(), userToken)
        || !AppData()->FeatureFlags.GetEnableDatabaseAdmin();
}

TEvTxProxySchemeCache::TEvNavigateKeySet* MakeArtifactDirListingRequest(const TString& databaseName) {
    auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
    request->DatabaseName = databaseName;
    auto& entry = request->ResultSet.emplace_back();
    entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpList;
    entry.Path = SplitPath(NUdfStore::GetUdfStorePrefix() + "/artifacts");
    return new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release());
}

bool ParseArtifactDirListing(
    const NSchemeCache::TSchemeCacheNavigate& response,
    TVector<TString>& cpuSpecs)
{
    cpuSpecs.clear();
    if (response.ResultSet.size() != 1) {
        return false;
    }
    const auto& entry = response.ResultSet.front();
    if (entry.Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
        // Nothing has been compiled on this cluster yet, so there is no
        // directory and no platform to report.
        return entry.Status == NSchemeCache::TSchemeCacheNavigate::EStatus::PathErrorUnknown;
    }
    if (!entry.ListNodeEntry) {
        return true;
    }
    for (const auto& child : entry.ListNodeEntry->Children) {
        if (child.Name.empty() || child.Name.EndsWith("_chunks")) {
            continue;
        }
        cpuSpecs.push_back(child.Name);
    }
    return true;
}

Ydb::Udf::ModuleKind ToProtoKind(EUdfType type) {
    switch (type) {
        case EUdfType::WASM:
            return Ydb::Udf::UDF;
        case EUdfType::LIBRARY:
            return Ydb::Udf::LIBRARY;
        case EUdfType::NATIVE_UNSAFE:
            // Native modules predate this API and it cannot manage them, but a
            // list must still be able to mention what occupies a name.
            return Ydb::Udf::MODULE_KIND_UNSPECIFIED;
    }
    return Ydb::Udf::MODULE_KIND_UNSPECIFIED;
}

bool FromProtoKind(Ydb::Udf::ModuleKind kind, EUdfType& type) {
    switch (kind) {
        case Ydb::Udf::UDF:
            type = EUdfType::WASM;
            return true;
        case Ydb::Udf::LIBRARY:
            type = EUdfType::LIBRARY;
            return true;
        default:
            return false;
    }
}

Ydb::Udf::CompileStatus ToProtoCompileStatus(ECompileStatus status) {
    switch (status) {
        case ECompileStatus::Pending:
            return Ydb::Udf::PENDING;
        case ECompileStatus::Compiling:
            return Ydb::Udf::COMPILING;
        case ECompileStatus::Ready:
            return Ydb::Udf::READY;
        case ECompileStatus::Failed:
            return Ydb::Udf::FAILED;
    }
    return Ydb::Udf::COMPILE_STATUS_UNSPECIFIED;
}

bool FromProtoCompileStatus(Ydb::Udf::CompileStatus status, ECompileStatus& result) {
    switch (status) {
        case Ydb::Udf::PENDING:
            result = ECompileStatus::Pending;
            return true;
        case Ydb::Udf::COMPILING:
            result = ECompileStatus::Compiling;
            return true;
        case Ydb::Udf::READY:
            result = ECompileStatus::Ready;
            return true;
        case Ydb::Udf::FAILED:
            result = ECompileStatus::Failed;
            return true;
        default:
            return false;
    }
}

void FillModuleInfo(const NQuery::TModuleRow& row, Ydb::Udf::ModuleInfo& info) {
    info.set_name(row.Name);
    info.set_kind(ToProtoKind(row.Type));
    info.set_uid(row.Uid);
    info.set_md5(row.Md5);
    info.set_size(row.Size);
    info.set_version(row.Version);
    info.set_compile_status(ToProtoCompileStatus(row.CompileStatus));
    info.set_compile_error(row.CompileError);
    FillTimestamp(row.CreatedAt, *info.mutable_created_at());
    FillTimestamp(row.CompileFinishedAt, *info.mutable_compile_finished_at());
}

} // namespace NKikimr::NUdfApi
