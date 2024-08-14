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


TKqpQueryState::TQueryTxId::TQueryTxId(const TQueryTxId& other) {
    YQL_ENSURE(!Id);
    Id = other.Id;
}

TKqpQueryState::TQueryTxId& TKqpQueryState::TQueryTxId::operator=(const TQueryTxId& id) {
    YQL_ENSURE(!Id);
    Id = id.Id;
    return *this;
}

void TKqpQueryState::TQueryTxId::SetValue(const TTxId& id) {
    YQL_ENSURE(!Id);
    Id = id.Id;
}

TTxId TKqpQueryState::TQueryTxId::GetValue() {
    return Id ? *Id : TTxId();
}

void TKqpQueryState::TQueryTxId::Reset() {
    Id = TTxId();
}

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

void TKqpQueryState::FillViews(const google::protobuf::RepeatedPtrField< ::NKqpProto::TKqpTableInfo>& views) {
    for (const auto& view : views) {
        const auto& pathId = view.GetTableId();
        const auto schemaVersion = view.GetSchemaVersion();
        auto [it, isInserted] = TableVersions.emplace(TTableId(pathId.GetOwnerId(), pathId.GetTableId()), schemaVersion);
        if (!isInserted) {
            Y_ENSURE(it->second == schemaVersion);
        }
    }
}

std::unique_ptr<TEvTxProxySchemeCache::TEvNavigateKeySet> TKqpQueryState::BuildNavigateKeySet() {
    TableVersions.clear();

    for (const auto& tx : PreparedQuery->GetPhysicalQuery().GetTransactions()) {
        FillTables(tx);
    }
    FillViews(PreparedQuery->GetPhysicalQuery().GetViewInfos());

    auto navigate = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
    navigate->DatabaseName = Database;
    if (HasUserToken()) {
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

    CompileStats = ev->Stats;
    PreparedQuery = CompileResult->PreparedQuery;
    if (ev->ReplayMessage) {
        ReplayMessage = *ev->ReplayMessage;
    }
    if (!CommandTagName) {
        CommandTagName = CompileResult->CommandTagName;
    }
    for (const auto& param : PreparedQuery->GetParameters()) {
        const auto& ast = CompileResult->Ast;
        if (!ast || !ast->PgAutoParamValues || !ast->PgAutoParamValues->contains(param.GetName())) {
            ResultParams.push_back(param);
        }
    }
    return true;
}

bool TKqpQueryState::SaveAndCheckParseResult(TEvKqp::TEvParseResponse&& ev) {
    Statements = std::move(ev.AstStatements);
    CurrentStatementId = 0;
    Orbit = std::move(ev.Orbit);
    CommandTagName = Statements.back().CommandTagName;
    return true;
}

bool TKqpQueryState::SaveAndCheckSplitResult(TEvKqp::TEvSplitResponse* ev) {
    SplittedExprs = std::move(ev->Exprs);
    SplittedWorld = std::move(ev->World);
    SplittedCtx = std::move(ev->Ctx);
    NextSplittedExpr = -1;
    return true;
}

std::unique_ptr<TEvKqp::TEvCompileRequest> TKqpQueryState::BuildCompileRequest(std::shared_ptr<std::atomic<bool>> cookie, const TGUCSettings::TPtr& gUCSettingsPtr) {
    TMaybe<TKqpQueryId> query;
    TMaybe<TString> uid;

    TKqpQuerySettings settings(GetType());
    settings.DocumentApiRestricted = IsDocumentApiRestricted_;
    settings.IsInternalCall = IsInternalCall();
    settings.Syntax = GetSyntax();

    bool keepInCache = false;
    bool perStatementResult = HasImplicitTx();
    TGUCSettings gUCSettings = gUCSettingsPtr ? *gUCSettingsPtr : TGUCSettings();
    switch (GetAction()) {
        case NKikimrKqp::QUERY_ACTION_EXECUTE:
            query = TKqpQueryId(Cluster, Database, GetQuery(), settings, GetQueryParameterTypes(), gUCSettings);
            keepInCache = GetQueryKeepInCache() && query->IsSql();
            break;

        case NKikimrKqp::QUERY_ACTION_PREPARE:
            query = TKqpQueryId(Cluster, Database, GetQuery(), settings, GetQueryParameterTypes(), gUCSettings);
            keepInCache = query->IsSql();
            break;

        case NKikimrKqp::QUERY_ACTION_EXECUTE_PREPARED:
            uid = GetPreparedQuery();
            keepInCache = GetQueryKeepInCache();
            break;

        case NKikimrKqp::QUERY_ACTION_EXPLAIN:
            query = TKqpQueryId(Cluster, Database, GetQuery(), settings, GetQueryParameterTypes(), gUCSettings);
            keepInCache = false;
            break;

        default:
            YQL_ENSURE(false);
    }

    auto compileDeadline = QueryDeadlines.TimeoutAt;
    if (QueryDeadlines.CancelAt) {
        compileDeadline = Min(compileDeadline, QueryDeadlines.CancelAt);
    }

    bool isQueryActionPrepare = GetAction() == NKikimrKqp::QUERY_ACTION_PREPARE;

    TMaybe<TQueryAst> statementAst;
    if (!Statements.empty()) {
        YQL_ENSURE(CurrentStatementId < Statements.size());
        statementAst = Statements[CurrentStatementId];
    }

    return std::make_unique<TEvKqp::TEvCompileRequest>(UserToken, uid, std::move(query), keepInCache,
        isQueryActionPrepare, perStatementResult, compileDeadline, DbCounters, gUCSettingsPtr, ApplicationName, std::move(cookie),
        UserRequestContext, std::move(Orbit), TempTablesState, GetCollectDiagnostics(), statementAst);
}

std::unique_ptr<TEvKqp::TEvRecompileRequest> TKqpQueryState::BuildReCompileRequest(std::shared_ptr<std::atomic<bool>> cookie, const TGUCSettings::TPtr& gUCSettingsPtr) {
    YQL_ENSURE(CompileResult);
    TMaybe<TKqpQueryId> query;
    TMaybe<TString> uid;

    TKqpQuerySettings settings(GetType());
    settings.DocumentApiRestricted = IsDocumentApiRestricted_;
    settings.IsInternalCall = IsInternalCall();
    settings.Syntax = GetSyntax();

    TGUCSettings gUCSettings = gUCSettingsPtr ? *gUCSettingsPtr : TGUCSettings();

    switch (GetAction()) {
        case NKikimrKqp::QUERY_ACTION_EXPLAIN:
        case NKikimrKqp::QUERY_ACTION_EXECUTE:
            query = TKqpQueryId(Cluster, Database, GetQuery(), settings, GetQueryParameterTypes(), gUCSettings);
            break;

        case NKikimrKqp::QUERY_ACTION_PREPARE:
            query = TKqpQueryId(Cluster, Database, GetQuery(), settings, GetQueryParameterTypes(), gUCSettings);
            break;

        case NKikimrKqp::QUERY_ACTION_EXECUTE_PREPARED:
            uid = GetPreparedQuery();
            break;

        default:
            YQL_ENSURE(false);
    }

    auto compileDeadline = QueryDeadlines.TimeoutAt;
    bool isQueryActionPrepare = GetAction() == NKikimrKqp::QUERY_ACTION_PREPARE;
    if (QueryDeadlines.CancelAt) {
        compileDeadline = Min(compileDeadline, QueryDeadlines.CancelAt);
    }

    TMaybe<TQueryAst> statementAst;
    if (!Statements.empty()) {
        YQL_ENSURE(CurrentStatementId < Statements.size());
        statementAst = Statements[CurrentStatementId];
    }

    return std::make_unique<TEvKqp::TEvRecompileRequest>(UserToken, CompileResult->Uid, query, isQueryActionPrepare,
        compileDeadline, DbCounters, gUCSettingsPtr, ApplicationName, std::move(cookie), UserRequestContext, std::move(Orbit), TempTablesState,
        statementAst);
}

std::unique_ptr<TEvKqp::TEvCompileRequest> TKqpQueryState::BuildSplitRequest(std::shared_ptr<std::atomic<bool>> cookie, const TGUCSettings::TPtr& gUCSettingsPtr) {
    auto request = BuildCompileRequest(cookie, gUCSettingsPtr);
    request->Split = true;
    return request;
}

std::unique_ptr<TEvKqp::TEvCompileRequest> TKqpQueryState::BuildCompileSplittedRequest(std::shared_ptr<std::atomic<bool>> cookie, const TGUCSettings::TPtr& gUCSettingsPtr) {
    TMaybe<TKqpQueryId> query;
    TMaybe<TString> uid;

    TKqpQuerySettings settings(GetType());
    settings.DocumentApiRestricted = IsDocumentApiRestricted_;
    settings.IsInternalCall = IsInternalCall();
    settings.Syntax = GetSyntax();
    TGUCSettings gUCSettings = gUCSettingsPtr ? *gUCSettingsPtr : TGUCSettings();

    switch (GetAction()) {
        case NKikimrKqp::QUERY_ACTION_EXECUTE:
            query = TKqpQueryId(Cluster, Database, GetQuery(), settings, GetQueryParameterTypes(), gUCSettings);
            break;
        default:
            YQL_ENSURE(false);
    }

    auto compileDeadline = QueryDeadlines.TimeoutAt;
    if (QueryDeadlines.CancelAt) {
        compileDeadline = Min(compileDeadline, QueryDeadlines.CancelAt);
    }

    const bool perStatementResult = !HasTxControl() && GetAction() == NKikimrKqp::QUERY_ACTION_EXECUTE;

    TMaybe<TQueryAst> statementAst;
    if (!Statements.empty()) {
        YQL_ENSURE(CurrentStatementId < Statements.size());
        statementAst = Statements[CurrentStatementId];
    }

    return std::make_unique<TEvKqp::TEvCompileRequest>(UserToken, uid, std::move(query), false,
        false, perStatementResult, compileDeadline, DbCounters, gUCSettingsPtr, ApplicationName, std::move(cookie),
        UserRequestContext, std::move(Orbit), TempTablesState, GetCollectDiagnostics(), statementAst,
        false, SplittedCtx.Get(), SplittedExprs.at(NextSplittedExpr));
}

bool TKqpQueryState::ProcessingLastStatementPart() {
    return SplittedExprs.empty() || (NextSplittedExpr + 1 >= static_cast<int>(SplittedExprs.size()));
}

bool TKqpQueryState::PrepareNextStatementPart() {
    QueryData = {};
    PreparedQuery = {};
    CompileResult = {};
    CurrentTx = 0;
    TableVersions = {};
    MaxReadType = ETableReadType::Other;
    Commit = false;
    Commited = false;
    TopicOperations = {};
    ReplayMessage = {};

    if (ProcessingLastStatementPart()) {
        SplittedWorld.Reset();
        SplittedExprs.clear();
        SplittedCtx.Reset();
        NextSplittedExpr = -1;
        return false;
    }

    ++NextSplittedExpr;
    return true;
}

void TKqpQueryState::AddOffsetsToTransaction() {
    YQL_ENSURE(HasTopicOperations());

    const auto& operations = GetTopicOperations();

    TMaybe<TString> consumer;
    if (operations.HasConsumer()) {
        consumer = operations.GetConsumer();
    }

    TMaybe<ui32> supportivePartition;
    if (operations.HasSupportivePartition()) {
        supportivePartition = operations.GetSupportivePartition();
    }

    TopicOperations = NTopic::TTopicOperations();

    for (auto& topic : operations.GetTopics()) {
        auto path = CanonizePath(NPersQueue::GetFullTopicPath(TlsActivationContext->AsActorContext(),
            GetDatabase(), topic.path()));

        for (auto& partition : topic.partitions()) {
            if (partition.partition_offsets().empty()) {
                TopicOperations.AddOperation(path, partition.partition_id(), supportivePartition);
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
    if (HasUserToken()) {
        navigate->UserToken = UserToken;
    }
    navigate->Cookie = QueryId;
    return navigate;
}

bool TKqpQueryState::HasUserToken() const
{
    return UserToken && !UserToken->GetSerializedToken().empty();
}

bool TKqpQueryState::IsAccessDenied(const NSchemeCache::TSchemeCacheNavigate& response, TString& message) {
    if (!HasUserToken()) {
        return false;
    }

    auto checkAccessDenied = [&] (const NSchemeCache::TSchemeCacheNavigate::TEntry& result) {
        static const auto selectRowRights = NACLib::EAccessRights::SelectRow;
        static const auto accessAttributesRights = NACLib::EAccessRights::ReadAttributes | NACLib::EAccessRights::WriteAttributes;
        // in future check right UseConsumer
        return result.SecurityObject && !(result.SecurityObject->CheckAccess(selectRowRights, *UserToken) || result.SecurityObject->CheckAccess(accessAttributesRights, *UserToken));
    };
    // don't build message string on success path
    bool denied = std::any_of(response.ResultSet.begin(), response.ResultSet.end(), checkAccessDenied);

    if (!denied) {
        return false;
    }

    TStringBuilder builder;
    builder << "Access for topic(s)";
    for (auto& result : response.ResultSet) {
        if (result.Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
            continue;
        }

        if (checkAccessDenied(result)) {
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

bool TKqpQueryState::HasImplicitTx() const {
    if (HasTxControl()) {
        return false;
    }

    const NKikimrKqp::EQueryAction action = RequestEv->GetAction();
    if (action != NKikimrKqp::QUERY_ACTION_EXECUTE) {
        return false;
    }

    const NKikimrKqp::EQueryType queryType = RequestEv->GetType();
    if (queryType != NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY &&
        queryType != NKikimrKqp::QUERY_TYPE_SQL_GENERIC_SCRIPT &&
        queryType != NKikimrKqp::QUERY_TYPE_SQL_GENERIC_CONCURRENT_QUERY)
    {
        return false;
    }

    return true;
}

}
