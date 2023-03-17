#include "kqp_query_state.h"

namespace NKikimr::NKqp {

using namespace NSchemeCache;

#define LOG_C(msg) LOG_CRIT_S(*TlsActivationContext, NKikimrServices::KQP_SESSION, msg)
#define LOG_E(msg) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::KQP_SESSION, msg)
#define LOG_W(msg) LOG_WARN_S(*TlsActivationContext, NKikimrServices::KQP_SESSION, msg)
#define LOG_N(msg) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::KQP_SESSION, msg)
#define LOG_I(msg) LOG_INFO_S(*TlsActivationContext, NKikimrServices::KQP_SESSION, msg)
#define LOG_D(msg) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::KQP_SESSION, msg)
#define LOG_T(msg) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::KQP_SESSION, msg)

bool TKqpQueryState::EnsureTableVersions(const TEvTxProxySchemeCache::TEvNavigateKeySetResult& response) {
    Y_ENSURE(response.Request);
    const auto& navigate = *response.Request;

    for (const auto& entry : navigate.ResultSet) {
        switch (entry.Status) {
            case TSchemeCacheNavigate::EStatus::Ok: {
                auto expectedVersion = TableVersions.FindPtr(TTableId(entry.TableId.PathId));
                if (!expectedVersion) {
                    LOG_W("Unexpected tableId in scheme cache navigate reply"
                        << ", tableId: " << entry.TableId);
                    continue;
                }

                if (!*expectedVersion) {
                    // Do not check tables with zero version.
                    continue;
                }

                if (entry.TableId.SchemaVersion && entry.TableId.SchemaVersion != *expectedVersion) {
                    LOG_I("Scheme version mismatch"
                        << ", pathId: " << entry.TableId.PathId
                        << ", expected version: " << *expectedVersion
                        << ", actual version: " << entry.TableId.SchemaVersion);
                    return false;
                }

                break;
            }

            case TSchemeCacheNavigate::EStatus::PathErrorUnknown:
            case TSchemeCacheNavigate::EStatus::PathNotTable:
            case TSchemeCacheNavigate::EStatus::TableCreationNotComplete:
                LOG_I("Scheme error"
                    << ", pathId: " << entry.TableId.PathId
                    << ", status: " << entry.Status);
                return false;

            case TSchemeCacheNavigate::EStatus::LookupError:
            case TSchemeCacheNavigate::EStatus::RedirectLookupError:
                // Transient error, do not invalidate the query.
                // Hard validation will be performed later during the query execution.
                break;

            default:
                // Unexpected reply, do not invalidate the query as it may block the query execution.
                // Hard validation will be performed later during the query execution.
                LOG_E("Unexpected reply from scheme cache"
                    << ", pathId: " << entry.TableId.PathId
                    << ", status: " << entry.Status);
                break;
        }
    }

    return true;
}


std::unique_ptr<TEvTxProxySchemeCache::TEvNavigateKeySet> TKqpQueryState::BuildNavigateKeySet() {
    TableVersions.clear();

    for (const auto& tx : PreparedQuery->GetPhysicalQuery().GetTransactions()) {
        FillTables(tx);
    }

    auto navigate = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
    navigate->DatabaseName = Database;
    if (UserToken && !UserToken->GetSerializedToken().empty()) {
        navigate->UserToken = UserToken;
    }

    navigate->Cookie = QueryId;

    for (const auto& [tableId, _] : TableVersions) {
        NSchemeCache::TSchemeCacheNavigate::TEntry entry;
        entry.TableId = tableId;
        entry.RequestType = NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByTableId;
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::EOp::OpTable;
        entry.SyncVersion = false;
        entry.ShowPrivatePath = true;
        navigate->ResultSet.emplace_back(entry);
    }

    return std::make_unique<TEvTxProxySchemeCache::TEvNavigateKeySet>(navigate.Release());
}


bool TKqpQueryState::SaveAndCheckCompileResult(TEvKqp::TEvCompileResponse* ev) {
    CompileResult = ev->CompileResult;
    YQL_ENSURE(CompileResult);
    MaxReadType = CompileResult->MaxReadType;
    Orbit = std::move(ev->Orbit);

    if (CompileResult->Status != Ydb::StatusIds::SUCCESS)
        return false;

    YQL_ENSURE(CompileResult->PreparedQuery);
    const ui32 compiledVersion = CompileResult->PreparedQuery->GetVersion();
    YQL_ENSURE(compiledVersion == NKikimrKqp::TPreparedQuery::VERSION_PHYSICAL_V1,
        "Unexpected prepared query version: " << compiledVersion);

    CompileStats.Swap(&ev->Stats);
    PreparedQuery = CompileResult->PreparedQuery;
    return true;
}

std::unique_ptr<TEvKqp::TEvCompileRequest> TKqpQueryState::BuildCompileRequest() {
    TMaybe<TKqpQueryId> query;
    TMaybe<TString> uid;

    bool keepInCache = false;
    switch (GetAction()) {
        case NKikimrKqp::QUERY_ACTION_EXECUTE:
            query = TKqpQueryId(Cluster, Database, GetQuery(), GetType());
            keepInCache = GetQueryKeepInCache() && query->IsSql();
            break;

        case NKikimrKqp::QUERY_ACTION_PREPARE:
            query = TKqpQueryId(Cluster, Database, GetQuery(), GetType());
            keepInCache = query->IsSql();
            break;

        case NKikimrKqp::QUERY_ACTION_EXECUTE_PREPARED:
            uid = GetPreparedQuery();
            keepInCache = GetQueryKeepInCache();
            break;

        default:
            YQL_ENSURE(false);
    }

    if (query) {
        query->Settings.DocumentApiRestricted = IsDocumentApiRestricted_;
    }

    auto compileDeadline = QueryDeadlines.TimeoutAt;
    if (QueryDeadlines.CancelAt) {
        compileDeadline = Min(compileDeadline, QueryDeadlines.CancelAt);
    }

    return std::make_unique<TEvKqp::TEvCompileRequest>(UserToken, uid,
        std::move(query), keepInCache, compileDeadline, DbCounters, std::move(Orbit));
}

std::unique_ptr<TEvKqp::TEvRecompileRequest> TKqpQueryState::BuildReCompileRequest() {
    YQL_ENSURE(CompileResult);
    TMaybe<TKqpQueryId> query;
    TMaybe<TString> uid;

    switch (GetAction()) {
        case NKikimrKqp::QUERY_ACTION_EXECUTE:
            query = TKqpQueryId(Cluster, Database, GetQuery(), GetType());
            break;

        case NKikimrKqp::QUERY_ACTION_PREPARE:
            query = TKqpQueryId(Cluster, Database, GetQuery(), GetType());
            break;

        case NKikimrKqp::QUERY_ACTION_EXECUTE_PREPARED:
            uid = GetPreparedQuery();
            break;

        default:
            YQL_ENSURE(false);
    }

    if (query) {
        query->Settings.DocumentApiRestricted = IsDocumentApiRestricted_;
    }

    auto compileDeadline = QueryDeadlines.TimeoutAt;
    if (QueryDeadlines.CancelAt) {
        compileDeadline = Min(compileDeadline, QueryDeadlines.CancelAt);
    }

    return std::make_unique<TEvKqp::TEvRecompileRequest>(UserToken, CompileResult->Uid,
        CompileResult->Query, compileDeadline, DbCounters, std::move(Orbit));
}

}
