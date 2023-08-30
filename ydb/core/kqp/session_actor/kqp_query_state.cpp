#include "kqp_query_state.h"

#include <ydb/library/persqueue/topic_parser/topic_parser.h>

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

    TKqpQuerySettings settings(GetType());
    settings.DocumentApiRestricted = IsDocumentApiRestricted_;
    settings.IsInternalCall = IsInternalCall();
    settings.Syntax = GetSyntax();

    bool keepInCache = false;
    switch (GetAction()) {
        case NKikimrKqp::QUERY_ACTION_EXECUTE:
            query = TKqpQueryId(Cluster, Database, GetQuery(), settings, GetQueryParameterTypes());
            keepInCache = GetQueryKeepInCache() && query->IsSql();
            break;

        case NKikimrKqp::QUERY_ACTION_PREPARE:
            query = TKqpQueryId(Cluster, Database, GetQuery(), settings, GetQueryParameterTypes());
            keepInCache = query->IsSql();
            break;

        case NKikimrKqp::QUERY_ACTION_EXECUTE_PREPARED:
            uid = GetPreparedQuery();
            keepInCache = GetQueryKeepInCache();
            break;

        case NKikimrKqp::QUERY_ACTION_EXPLAIN:
            query = TKqpQueryId(Cluster, Database, GetQuery(), settings, GetQueryParameterTypes());
            keepInCache = false;
            break;

        default:
            YQL_ENSURE(false);
    }

    auto compileDeadline = QueryDeadlines.TimeoutAt;
    if (QueryDeadlines.CancelAt) {
        compileDeadline = Min(compileDeadline, QueryDeadlines.CancelAt);
    }

    return std::make_unique<TEvKqp::TEvCompileRequest>(UserToken, uid,
        std::move(query), keepInCache, compileDeadline, DbCounters, std::move(Orbit), TempTablesState);
}

std::unique_ptr<TEvKqp::TEvRecompileRequest> TKqpQueryState::BuildReCompileRequest() {
    YQL_ENSURE(CompileResult);
    TMaybe<TKqpQueryId> query;
    TMaybe<TString> uid;

    TKqpQuerySettings settings(GetType());
    settings.DocumentApiRestricted = IsDocumentApiRestricted_;
    settings.IsInternalCall = IsInternalCall();
    settings.Syntax = GetSyntax();

    switch (GetAction()) {
        case NKikimrKqp::QUERY_ACTION_EXECUTE:
            query = TKqpQueryId(Cluster, Database, GetQuery(), settings, GetQueryParameterTypes());
            break;

        case NKikimrKqp::QUERY_ACTION_PREPARE:
            query = TKqpQueryId(Cluster, Database, GetQuery(), settings, GetQueryParameterTypes());
            break;

        case NKikimrKqp::QUERY_ACTION_EXECUTE_PREPARED:
            uid = GetPreparedQuery();
            break;

        default:
            YQL_ENSURE(false);
    }

    auto compileDeadline = QueryDeadlines.TimeoutAt;
    if (QueryDeadlines.CancelAt) {
        compileDeadline = Min(compileDeadline, QueryDeadlines.CancelAt);
    }

    return std::make_unique<TEvKqp::TEvRecompileRequest>(UserToken, CompileResult->Uid,
        CompileResult->Query, compileDeadline, DbCounters, std::move(Orbit), TempTablesState);
}

void TKqpQueryState::AddOffsetsToTransaction() {
    YQL_ENSURE(HasTopicOperations());

    const auto& operations = GetTopicOperations();

    TMaybe<TString> consumer;
    if (operations.HasConsumer())
        consumer = operations.GetConsumer();

    TopicOperations = NTopic::TTopicOperations();

    for (auto& topic : operations.GetTopics()) {
        auto path = CanonizePath(NPersQueue::GetFullTopicPath(TlsActivationContext->AsActorContext(),
            GetDatabase(), topic.path()));

        for (auto& partition : topic.partitions()) {
            if (partition.partition_offsets().empty()) {
                TopicOperations.AddOperation(path, partition.partition_id());
            } else {
                for (auto& range : partition.partition_offsets()) {
                    YQL_ENSURE(consumer.Defined());

                    TopicOperations.AddOperation(path, partition.partition_id(), *consumer, range);
                }
            }
        }
    }
}

bool TKqpQueryState::TryMergeTopicOffsets(const NTopic::TTopicOperations &operations, TString& message) {
    try {
        TxCtx->TopicOperations.Merge(operations);
        return true;
    } catch (const NTopic::TOffsetsRangeIntersectExpection &ex) {
        message = ex.what();
        return false;
    }
}

std::unique_ptr<NSchemeCache::TSchemeCacheNavigate> TKqpQueryState::BuildSchemeCacheNavigate() {
    auto navigate = std::make_unique<NSchemeCache::TSchemeCacheNavigate>();
    navigate->DatabaseName = CanonizePath(GetDatabase());

    const auto& operations = GetTopicOperations();
    TMaybe<TString> consumer;
    if (operations.HasConsumer())
        consumer = operations.GetConsumer();

    TopicOperations.FillSchemeCacheNavigate(*navigate, std::move(consumer));
    navigate->UserToken = UserToken;
    navigate->Cookie = QueryId;
    return navigate;
}

bool TKqpQueryState::IsAccessDenied(const NSchemeCache::TSchemeCacheNavigate& response, TString& message) {
    auto rights = NACLib::EAccessRights::ReadAttributes | NACLib::EAccessRights::WriteAttributes;
    // don't build message string on success path
    bool denied = std::any_of(response.ResultSet.begin(), response.ResultSet.end(), [&] (auto& result) {
        return result.SecurityObject && !result.SecurityObject->CheckAccess(rights, *UserToken);
    });

    if (!denied) {
        return false;
    }

    TStringBuilder builder;
    builder << "Access for topic(s)";
    for (auto& result : response.ResultSet) {
        if (result.Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
            continue;
        }

        if (result.SecurityObject && !result.SecurityObject->CheckAccess(rights, *UserToken)) {
            builder << " '" << JoinPath(result.Path) << "'";
        }
    }

    builder << " is denied for subject '" << UserToken->GetUserSID() << "'";
    message = std::move(builder);

    return true;
}

bool TKqpQueryState::HasErrors(const NSchemeCache::TSchemeCacheNavigate& response, TString& message) {
    if (response.ErrorCount == 0) {
        return false;
    }

    TStringBuilder builder;

    builder << "Unable to navigate:";
    for (const auto& result : response.ResultSet) {
        if (result.Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
            builder << "path: '" << JoinPath(result.Path) << "' status: " << result.Status;
        }
    }
    message = std::move(builder);

    return true;
}

}
