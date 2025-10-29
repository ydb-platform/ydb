#include "streaming_queries.h"

#include <ydb/core/kqp/common/events/script_executions.h>
#include <ydb/core/kqp/common/kqp_script_executions.h>
#include <ydb/core/kqp/gateway/behaviour/streaming_query/common/utils.h>
#include <ydb/core/kqp/workload_service/actors/actors.h>
#include <ydb/core/sys_view/common/registry.h>
#include <ydb/core/sys_view/common/scan_actor_base_impl.h>
#include <ydb/library/query_actor/query_actor.h>

#include <contrib/libs/fmt/include/fmt/format.h>

#include <library/cpp/protobuf/json/json2proto.h>

namespace NKikimr::NSysView {

namespace {

#define LOG_T(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::SYSTEM_VIEWS, "[StreamingQueries] [SysView] " << LogPrefix() << stream)
#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::SYSTEM_VIEWS, "[StreamingQueries] [SysView] " << LogPrefix() << stream)
#define LOG_I(stream) LOG_INFO_S(*TlsActivationContext, NKikimrServices::SYSTEM_VIEWS, "[StreamingQueries] [SysView] " << LogPrefix() << stream)
#define LOG_N(stream) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::SYSTEM_VIEWS, "[StreamingQueries] [SysView] " << LogPrefix() << stream)
#define LOG_W(stream) LOG_WARN_S(*TlsActivationContext, NKikimrServices::SYSTEM_VIEWS, "[StreamingQueries] [SysView] " << LogPrefix() << stream)
#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::SYSTEM_VIEWS, "[StreamingQueries] [SysView] " << LogPrefix() << stream)
#define LOG_C(stream) LOG_CRIT_S(*TlsActivationContext, NKikimrServices::SYSTEM_VIEWS, "[StreamingQueries] [SysView] " << LogPrefix() << stream)

using namespace fmt::literals;

constexpr char STREAMING_QUERIES_TABLE[] = ".metadata/streaming/queries";

struct TEvPrivate {
    // Event ids
    enum EEv : ui32 {
        EvStart = EventSpaceBegin(TEvents::ES_PRIVATE),

        EvCheckStreamingQueriesTables = EvStart,
        EvFetchStreamingQueriesResult,
        EvDescribeStreamingQueriesResult,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");

    struct TEvCheckStreamingQueriesTables : public TEventLocal<TEvCheckStreamingQueriesTables, EvCheckStreamingQueriesTables> {
        TEvCheckStreamingQueriesTables(Ydb::StatusIds::StatusCode status, bool tablesExist, NYql::TIssues&& issues)
            : Status(status)
            , TablesExist(tablesExist)
            , Issues(std::move(issues))
        {}

        const Ydb::StatusIds::StatusCode Status;
        const bool TablesExist = false;
        const NYql::TIssues Issues;
    };

    struct TEvFetchStreamingQueriesResult : public TEventLocal<TEvFetchStreamingQueriesResult, EvFetchStreamingQueriesResult> {
        struct TQuery {
            TString Path;
            NKikimrKqp::TStreamingQueryState State;
        };

        struct TInfo {
            std::vector<TQuery> Queries;
            bool Truncated = false;
            std::optional<TString> PageToken;
        };

        TEvFetchStreamingQueriesResult(Ydb::StatusIds::StatusCode status, TInfo&& info, NYql::TIssues&& issues)
            : Status(status)
            , Info(std::move(info))
            , Issues(std::move(issues))
        {}

        const Ydb::StatusIds::StatusCode Status;
        TInfo Info;
        const NYql::TIssues Issues;
    };

    struct TEvDescribeStreamingQueriesResult : public TEventLocal<TEvDescribeStreamingQueriesResult, EvDescribeStreamingQueriesResult> {
        TEvDescribeStreamingQueriesResult(Ydb::StatusIds::StatusCode status, std::unordered_map<TString, NKqp::TStreamingQuerySettings>&& infos, NYql::TIssues&& issues)
            : Status(status)
            , Infos(std::move(infos))
            , Issues(std::move(issues))
        {}

        const Ydb::StatusIds::StatusCode Status;
        std::unordered_map<TString, NKqp::TStreamingQuerySettings> Infos;
        const NYql::TIssues Issues;
    };
};

class TStreamingQueryFetcherActor final : public TQueryBase {
    using TBase = TQueryBase;

    static constexpr ui64 MAX_STREAMING_QUERIES_COUNT = 1000;

public:
    struct TRangeBound {
        TStringBuf Value;
        bool Inclusive = true;
    };

    struct TSettings {
        bool Reverse = false;
        std::optional<TRangeBound> From;
        std::optional<TRangeBound> To;
        std::optional<TString> PageToken;
        ui64 FreeSpace = 0;
    };

    using TRetry = TQueryRetryActor<TStreamingQueryFetcherActor, TEvPrivate::TEvFetchStreamingQueriesResult, TString, TSettings>;

    TStreamingQueryFetcherActor(const TString& databaseId, const TSettings& settings)
        : TBase(NKikimrServices::SYSTEM_VIEWS)
        , DatabaseId(databaseId)
        , Settings(settings)
    {
        SetOperationInfo(__func__, databaseId);
    }

private:
    void OnRunQuery() final {
        LOG_D("Start fetch streaming queries, Reverse: " << Settings.Reverse);

        TStringBuilder paramsDecl;
        TStringBuilder rangeFilter;
        NYdb::TParamsBuilder params;
        params
            .AddParam("$database_id")
                .Utf8(DatabaseId)
                .Build()
            .AddParam("$limit")
                .Uint64(MAX_STREAMING_QUERIES_COUNT + 1)
                .Build();

        if (const auto& from = Settings.From) {
            LOG_T("Filter From: " << from->Value << ", Inclusive: " << from->Inclusive);

            paramsDecl << "DECLARE $from AS Text;\n";
            rangeFilter << " AND query_path " << (from->Inclusive ? ">=" : ">") << " $from";
            params.AddParam("$from")
                .Utf8(TString(from->Value))
                .Build();
        }

        if (const auto& to = Settings.To) {
            LOG_T("Filter To: " << to->Value << ", Inclusive: " << to->Inclusive);

            paramsDecl << "DECLARE $to AS Text;\n";
            rangeFilter << " AND query_path " << (to->Inclusive ? "<=" : "<") << " $to";
            params.AddParam("$to")
                .Utf8(TString(to->Value))
                .Build();
        }

        if (const auto& pageToken = Settings.PageToken) {
            LOG_T("Setup page token: " << *pageToken);

            paramsDecl << "DECLARE $page_token AS Text;\n";
            rangeFilter << " AND query_path " << (Settings.Reverse ? "<" : ">") << " $page_token";
            params.AddParam("$page_token")
                .Utf8(*pageToken)
                .Build();
        }

        const TString sql = fmt::format(R"(
                DECLARE $database_id AS Text;
                DECLARE $limit AS Uint64;
                {params_decl}

                SELECT
                    query_path, state
                FROM `{table}`
                WHERE database_id = $database_id {range_filter}
                ORDER BY database_id, query_path {order}
                LIMIT $limit;
            )",
            "table"_a = STREAMING_QUERIES_TABLE,
            "params_decl"_a = paramsDecl,
            "range_filter"_a = rangeFilter,
            "order"_a = Settings.Reverse ? "DESC" : "ASC"
        );

        RunStreamQuery(sql, &params, std::min(Settings.FreeSpace, 60_MB));
    }

    void OnStreamResult(NYdb::TResultSet&& resultSet) final {
        using EColumns = NKqp::TStreamingQueryMeta::TColumns;

        NYdb::TResultSetParser result(resultSet);
        while (result.TryNextRow()) {
            if (Result.Queries.size() >= MAX_STREAMING_QUERIES_COUNT) {
                CancelFetchQuery();
                break;
            }

            const std::optional<TString>& queryPath = result.ColumnParser(EColumns::QueryPath).GetOptionalUtf8();
            if (!queryPath) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Streaming query path not found");
                return;
            }

            const auto& stateJsonString = result.ColumnParser(EColumns::State).GetOptionalJson();
            if (!stateJsonString) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, TStringBuilder() << "Streaming query '" << *queryPath << "' state not found");
                return;
            }

            FetchedSize += queryPath->size() + stateJsonString->size();
            if (!Result.Queries.empty() && FetchedSize > Settings.FreeSpace) {
                CancelFetchQuery();
                break;
            }

            NJson::TJsonValue stateJson;
            if (!NJson::ReadJsonTree(*stateJsonString, &stateJson)) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, TStringBuilder() << "Streaming query '" << *queryPath << "' state is corrupted");
                return;
            }

            auto& query = Result.Queries.emplace_back();

            try {
                NProtobufJson::Json2Proto(stateJson, query.State);
            } catch (const std::exception& e) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, TStringBuilder() << "Failed to parse streaming query '" << *queryPath << "' state: " << e.what());
                return;
            }

            query.Path = *queryPath;
            Result.PageToken = *queryPath;
        }
    }

    void OnQueryResult() final {
        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) final {
        Send(Owner, new TEvPrivate::TEvFetchStreamingQueriesResult(status, std::move(Result), std::move(issues)));
    }

private:
    void CancelFetchQuery() {
        Result.Truncated = true;
        CancelStreamQuery();
    }

private:
    const TString DatabaseId;
    const TSettings Settings;
    TEvPrivate::TEvFetchStreamingQueriesResult::TInfo Result;
    ui64 FetchedSize = 0;
};

template <typename TDerived>
class TSchemeDescribeActorBase : public TActorBootstrapped<TDerived> {
    using TBase = TActorBootstrapped<TDerived>;
    using TRetryPolicy = IRetryPolicy<>;
    using EStatus = NSchemeCache::TSchemeCacheNavigate::EStatus;

public:
    TSchemeDescribeActorBase(const TString& operationName, const TString& database, const std::vector<TString>& paths, TIntrusiveConstPtr<NACLib::TUserToken> userToken)
        : OperationName(operationName)
        , Database(database)
        , Paths(paths)
        , UserToken(std::move(userToken))
    {}

    void Bootstrap() {
        LOG_D("Bootstrap");
        StartRequest();

        TBase::Become(&TSchemeDescribeActorBase::StateFunc);
    }

    STRICT_STFUNC(StateFunc,
        sFunc(TEvents::TEvWakeup, StartRequest);
        hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
        hFunc(TEvents::TEvUndelivered, Handle);
    )

    void StartRequest() {
        WaitRetry = false;
        LOG_D("Describe #" << Paths.size() << " paths");

        auto request = std::make_unique<NSchemeCache::TSchemeCacheNavigate>();
        request->DatabaseName = Database;
        request->UserToken = UserToken;

        request->ResultSet.reserve(Paths.size());
        for (const auto& path : Paths) {
            auto& entry = request->ResultSet.emplace_back();
            entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
            entry.RequestType = NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByPath;
            entry.ShowPrivatePath = true;
            entry.Path = SplitPath(path);
            entry.SyncVersion = true;
            entry.Access = NACLib::DescribeSchema;
        }

        TBase::Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.release()), IEventHandle::FlagTrackDelivery);
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto& results = ev->Get()->Request->ResultSet;
        if (results.size() != Paths.size()) {
            FatalError(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected scheme cache response");
            return;
        }

        std::vector<TString> pathsToRetry;
        for (ui64 i = 0; i < results.size(); ++i) {
            const auto& path = Paths[i];
            const auto& result = results[i];
            LOG_D("Got scheme cache response for path '" << path << "': " << result.Status);

            switch (result.Status) {
                case EStatus::Unknown:
                case EStatus::PathNotTable:
                case EStatus::PathNotPath:
                case EStatus::RedirectLookupError:
                case EStatus::RootUnknown:
                case EStatus::PathErrorUnknown:
                case EStatus::AccessDenied: {
                    LOG_W("Path not found or access denied in SS: " << path << ", status: " << result.Status << ", user: '" << (UserToken ? UserToken->GetUserSID() : "<null>") << "'");
                    continue;
                }
                case EStatus::LookupError:
                case EStatus::TableCreationNotComplete: {
                    if (!ScheduleRetry(TStringBuilder() << "Retry error " << result.Status)) {
                        FatalError(Ydb::StatusIds::UNAVAILABLE, TStringBuilder() << "Retry limit exceeded on scheme error: " << result.Status);
                        return;
                    }

                    pathsToRetry.emplace_back(path);
                    continue;
                }
                case EStatus::Ok: {
                    if (!HandlePath(path, result)) {
                        return;
                    }
                }
            }
        }

        Paths = std::move(pathsToRetry);
        if (Paths.empty()) {
            Finish(Ydb::StatusIds::SUCCESS);
        }
    }

    void Handle(TEvents::TEvUndelivered::TPtr& ev) {
        const auto reason = ev->Get()->Reason;
        if (reason == TEvents::TEvUndelivered::ReasonActorUnknown && ScheduleRetry("Scheme service not found")) {
            return;
        }

        LOG_E("Scheme service is unavailable: " << ev->Get()->Reason);
        FatalError(Ydb::StatusIds::UNAVAILABLE, "Scheme service is unavailable");
    }

protected:
    virtual bool HandlePath(const TString& path, const NSchemeCache::TSchemeCacheNavigate::TEntry& entry) = 0;

    virtual void DoFinish(Ydb::StatusIds::StatusCode status) = 0;

protected:
    TString LogPrefix() const {
        return TStringBuilder() << "[" << OperationName << "] OwnerId: " << Owner << " ActorId: " << TBase::SelfId() << " Database: " << Database << ". ";
    }

    void Finish(Ydb::StatusIds::StatusCode status) {
        if (status == Ydb::StatusIds::SUCCESS) {
            LOG_D("Successfully finished");
        } else {
            LOG_W("Failed " << status << ", with issues: " << Issues.ToOneLineString());
        }

        DoFinish(status);
    }

    void FatalError(Ydb::StatusIds::StatusCode status, NYql::TIssues issues) {
        Issues.AddIssues(std::move(issues));
        Finish(status);
    }

    void FatalError(Ydb::StatusIds::StatusCode status, const TString& message) {
        FatalError(status, {NYql::TIssue(message)});
    }

private:
    void Registered(TActorSystem* sys, const TActorId& owner) final {
        TBase::Registered(sys, owner);
        Owner = owner;
    }

    bool ScheduleRetry(NYql::TIssues issues) {
        if (WaitRetry) {
            return true;
        }

        if (!RetryState) {
            RetryState = TRetryPolicy::GetExponentialBackoffPolicy(
                []() {
                    return ERetryErrorClass::ShortRetry;
                },
                TDuration::MilliSeconds(100),
                TDuration::MilliSeconds(500),
                TDuration::Seconds(1),
                std::numeric_limits<size_t>::max(),
                TDuration::Seconds(10)
            )->CreateRetryState();
        }

        if (const auto delay = RetryState->GetNextRetryDelay()) {
            LOG_W("Schedule retry for error: " << issues.ToOneLineString() << " in " << *delay);
            Issues.AddIssues(std::move(issues));
            TBase::Schedule(*delay, new TEvents::TEvWakeup());
            WaitRetry = true;
            return true;
        }

        return false;
    }

    bool ScheduleRetry(const TString& message) {
        return ScheduleRetry({NYql::TIssue(message)});
    }

private:
    const TString OperationName;
    const TString Database;
    std::vector<TString> Paths;
    const TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    bool WaitRetry = false;
    TRetryPolicy::IRetryState::TPtr RetryState;

protected:
    TActorId Owner;
    NYql::TIssues Issues;
};

class TStreamingQueriesTablesCheckerActor final : public TSchemeDescribeActorBase<TStreamingQueriesTablesCheckerActor> {
    using TBase = TSchemeDescribeActorBase<TStreamingQueriesTablesCheckerActor>;

public:
    TStreamingQueriesTablesCheckerActor(const TString& database)
        : TBase(__func__, database, {JoinPath({database, STREAMING_QUERIES_TABLE})}, MakeIntrusive<NACLib::TUserToken>(BUILTIN_ACL_METADATA, TVector<NACLib::TSID>{}))
    {}

protected:
    bool HandlePath(const TString& path, const NSchemeCache::TSchemeCacheNavigate::TEntry& entry) final {
        Y_UNUSED(path);

        TablesExist = entry.Kind == NSchemeCache::TSchemeCacheNavigate::KindTable;
        LOG_T("Streaming queries tables exists: " << TablesExist);

        return true;
    }

    void DoFinish(Ydb::StatusIds::StatusCode status) final {
        Send(Owner, new TEvPrivate::TEvCheckStreamingQueriesTables(status, TablesExist, std::move(Issues)));
    }

private:
    bool TablesExist = false;
};

class TStreamingQueryDescribeActor final : public TSchemeDescribeActorBase<TStreamingQueryDescribeActor> {
    using TBase = TSchemeDescribeActorBase<TStreamingQueryDescribeActor>;

public:
    TStreamingQueryDescribeActor(const TString& database, const std::vector<TString>& queryPaths, TIntrusiveConstPtr<NACLib::TUserToken> userToken)
        : TBase(__func__, database, queryPaths, std::move(userToken))
    {}

protected:
    bool HandlePath(const TString& path, const NSchemeCache::TSchemeCacheNavigate::TEntry& entry) final {
        if (entry.Kind != NSchemeCache::TSchemeCacheNavigate::KindStreamingQuery) {
            LOG_W("Path " << path << " exists, but it is not a streaming query: " << entry.Kind);
            return true;
        }

        if (!entry.StreamingQueryInfo) {
            FatalError(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected scheme cache response for ok status");
            return false;
        }

        const auto& description = entry.StreamingQueryInfo->Description;
        LOG_T("Found streaming query " << description.ShortDebugString());

        Infos[path].FromProto(description.GetProperties());
        return true;
    }

    void DoFinish(Ydb::StatusIds::StatusCode status) final {
        Send(Owner, new TEvPrivate::TEvDescribeStreamingQueriesResult(status, std::move(Infos), std::move(Issues)));
    }

private:
    std::unordered_map<TString, NKqp::TStreamingQuerySettings> Infos;
};

// Fetching pipeline:
// resolve database -> check tables -> fetch from queries table -> describe in SS -> resolve script executions

class TStreamingQueriesScan final : public TScanActorBase<TStreamingQueriesScan> {
    using TBase = TScanActorBase<TStreamingQueriesScan>;
    using TSchema = Schema::StreamingQueries;

    struct TQueryInfo {
        NKikimrKqp::TStreamingQueryState State;

        // Sys view columns values
        TString Status;
        TString Issues = "{}";
        TString Plan = "{}";
        TString Ast;
        TString Text;
        bool Run = false;
        TString ResourcePool;
        ui64 RetryCount = 0;
        TInstant LastFailAt;
        TInstant SuspendedUntil;
        TString LastExecutionId;
        TString PreviousExecutionIds = "{}";

        ui64 GetSize() const {
            return sizeof(TQueryInfo) + Status.size() + Issues.size() + Plan.size() + Ast.size() + Text.size() + ResourcePool.size() + LastExecutionId.size() + PreviousExecutionIds.size();
        }
    };

    using TExtractorValue = std::pair<TString, TQueryInfo>;

    class TExtractorsMap : public THashMap<NTable::TTag, std::function<TCell(const TExtractorValue&)>> {
    public:
        TExtractorsMap() {
            AddString<TSchema::Path>([](const TExtractorValue& p) { return p.first; });
            AddString<TSchema::Status>([](const TExtractorValue& p) { return p.second.Status; });
            AddString<TSchema::Issues>([](const TExtractorValue& p) { return p.second.Issues; });
            AddString<TSchema::Plan>([](const TExtractorValue& p) { return p.second.Plan; });
            AddString<TSchema::Ast>([](const TExtractorValue& p) { return p.second.Ast; });
            AddString<TSchema::Text>([](const TExtractorValue& p) { return p.second.Text; });
            Add<TSchema::Run, bool>([](const TExtractorValue& p) { return p.second.Run; });
            AddString<TSchema::ResourcePool>([](const TExtractorValue& p) { return p.second.ResourcePool; });
            Add<TSchema::RetryCount, ui64>([](const TExtractorValue& p) { return p.second.RetryCount; });
            AddOpt<TSchema::LastFailAt, ui64>([](const TExtractorValue& p) -> std::optional<ui64> {
                if (p.second.LastFailAt) {
                    return p.second.LastFailAt.MicroSeconds();
                }
                return std::nullopt;
            });
            AddOpt<TSchema::SuspendedUntil, ui64>([](const TExtractorValue& p) -> std::optional<ui64> {
                if (p.second.SuspendedUntil) {
                    return p.second.SuspendedUntil.MicroSeconds();
                }
                return std::nullopt;
            });
            AddString<TSchema::LastExecutionId>([](const TExtractorValue& p) { return p.second.LastExecutionId; });
            AddString<TSchema::PreviousExecutionIds>([](const TExtractorValue& p) { return p.second.PreviousExecutionIds; });
        }

    private:
        template <typename TCol, typename TType>
        void Add(std::function<TType(const TExtractorValue& p)> extractor) {
            insert({TCol::ColumnId, [extractor](const TExtractorValue& p) {
                return TCell::Make<TType>(extractor(p));
            }});
        }

        template <typename TCol, typename TType>
        void AddOpt(std::function<std::optional<TType>(const TExtractorValue& p)> extractor) {
            insert({TCol::ColumnId, [extractor](const TExtractorValue& p) {
                if (const auto& value = extractor(p)) {
                    return TCell::Make<TType>(*value);
                }
                return TCell();
            }});
        }

        template <typename TCol>
        void AddString(std::function<TString(const TExtractorValue& p)> textExtractor) {
            insert({TCol::ColumnId, [textExtractor](const TExtractorValue& p) {
                const auto& value = textExtractor(p);
                return TCell(value.data(), value.size());
            }});
        }
    };

    static constexpr ui64 MAX_SCRIPT_EXECUTION_RESOLVE_INFLIGHT = 10;

public:
    TStreamingQueriesScan(const TActorId& ownerId, ui32 scanId, const TString& database,
        const NKikimrSysView::TSysViewDescription& sysViewInfo, const TTableRange& tableRange,
        const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns,
        TIntrusiveConstPtr<NACLib::TUserToken> userToken, bool reverse)
        : TBase(ownerId, scanId, database, sysViewInfo, tableRange, columns)
        , UserToken(std::move(userToken))
        , Reverse(reverse)
    {
        for (const auto& column : Columns) {
            if (ScriptExecutionInfoRequired = IsIn({
                TSchema::Status::ColumnId, TSchema::Issues::ColumnId, TSchema::Plan::ColumnId,
                TSchema::Ast::ColumnId, TSchema::RetryCount::ColumnId, TSchema::LastFailAt::ColumnId,
                TSchema::SuspendedUntil::ColumnId
            }, column.Tag)) {
                break;
            }
        }
    }

    STFUNC(StateScan) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NKqp::TEvKqpCompute::TEvScanDataAck, Handle);
            hFunc(NKqp::NWorkload::TEvFetchDatabaseResponse, Handle);
            hFunc(TEvPrivate::TEvCheckStreamingQueriesTables, Handle);
            hFunc(TEvPrivate::TEvFetchStreamingQueriesResult, Handle);
            hFunc(TEvPrivate::TEvDescribeStreamingQueriesResult, Handle);
            hFunc(NKqp::TEvGetScriptExecutionOperationResponse, Handle);
            hFunc(NKqp::TEvKqp::TEvAbortExecution, HandleAbortExecution);
            cFunc(TEvents::TEvWakeup::EventType, HandleTimeout);
            cFunc(TEvents::TEvPoison::EventType, PassAway);
            default:
                LOG_C("NSysView::TStreamingQueriesScan: unexpected event " << ev->GetTypeRewrite());
        }
    }

    void Handle(NKqp::TEvKqpCompute::TEvScanDataAck::TPtr& ev) {
        FreeSpace = ev->Get()->FreeSpace;
        LOG_T("Received ack from " << ev->Sender << ", free space: " << FreeSpace);

        ContinueScan();
    }

    void Handle(NKqp::NWorkload::TEvFetchDatabaseResponse::TPtr& ev) {
        HasInflightOperation = false;

        if (const auto status = ev->Get()->Status; status != Ydb::StatusIds::SUCCESS) {
            const auto& issues = ev->Get()->Issues;
            LOG_E("Fetch database " << ev->Sender << " failed " << status << ", issues: " << issues.ToOneLineString());
            ReplyErrorAndDie(status, NKqp::AddRootIssue("Failed to fetch database info", issues));
            return;
        }

        DatabaseId = ev->Get()->DatabaseId;
        if (!DatabaseId) {
            InternalError(TStringBuilder() << "Fetch database " << ev->Sender << " failed, database id is empty");
            return;
        }

        LOG_D("Fetch database " << ev->Sender << " succeeded, database id: " << DatabaseId);
        ContinueScan();
    }

    void Handle(TEvPrivate::TEvCheckStreamingQueriesTables::TPtr& ev) {
        HasInflightOperation = false;

        if (const auto status = ev->Get()->Status; status != Ydb::StatusIds::SUCCESS) {
            const auto& issues = ev->Get()->Issues;
            LOG_E("Check streaming queries tables " << ev->Sender << " failed " << status << ", issues: " << issues.ToOneLineString());
            ReplyErrorAndDie(status, NKqp::AddRootIssue("Failed to check streaming queries tables", issues));
            return;
        }

        const auto tablesExist = ev->Get()->TablesExist;
        LOG_D("Check streaming queries tables " << ev->Sender << " succeeded, tables exists: " << tablesExist);

        CheckedTablesExistence = true;
        ListStreamingQueriesFinished = !tablesExist;
        ContinueScan();
    }

    void Handle(TEvPrivate::TEvFetchStreamingQueriesResult::TPtr& ev) {
        HasInflightOperation = false;

        if (const auto status = ev->Get()->Status; status != Ydb::StatusIds::SUCCESS) {
            const auto& issues = ev->Get()->Issues;
            LOG_E("Fetch streaming queries " << ev->Sender << " failed " << status << ", issues: " << issues.ToOneLineString());
            ReplyErrorAndDie(status, NKqp::AddRootIssue("Failed to fetch streaming queries info", issues));
            return;
        }

        auto& result = ev->Get()->Info;
        LOG_D("Fetched #" << result.Queries.size() << " streaming queries from " << ev->Sender
            << ", truncated: " << result.Truncated
            << ", page token: " << result.PageToken.value_or("<null>"));

        ListStreamingQueriesFinished = !result.Truncated;
        ListStreamingQueriesPageToken = result.PageToken;
        ListedQueries = std::move(result.Queries);
        ContinueScan();
    }

    void Handle(TEvPrivate::TEvDescribeStreamingQueriesResult::TPtr& ev) {
        HasInflightOperation = false;

        if (const auto status = ev->Get()->Status; status != Ydb::StatusIds::SUCCESS) {
            const auto& issues = ev->Get()->Issues;
            LOG_E("Describe streaming queries " << ev->Sender << " failed " << status << ", issues: " << issues.ToOneLineString());
            ReplyErrorAndDie(status, NKqp::AddRootIssue("Failed to describe streaming queries", issues));
            return;
        }

        const auto& infos = ev->Get()->Infos;
        LOG_D("Described #" << infos.size() << " streaming queries from " << ev->Sender);

        for (const auto& query : ListedQueries) {
            const auto it = infos.find(query.Path);
            if (it == infos.end()) {
                LOG_I("Describe query '" << query.Path << "' not found, it was removed or user " << (UserToken ? UserToken->GetUserSID() : "<null>") << " has no permissions");
                continue;
            }

            const auto& info = it->second;
            LOG_T("Described query: " << query.Path << ", state: " << query.State.ShortDebugString()
                << ", text: " << info.QueryText
                << ", run: " << info.Run
                << ", resource pool: " << info.ResourcePool);

            QueriesBatch[query.Path] = {
                .State = query.State,
                .Status = StatusToString(query.State.GetStatus()),
                .Text = info.QueryText,
                .Run = info.Run,
                .ResourcePool = info.ResourcePool,
                .LastExecutionId = query.State.GetCurrentExecutionId(),
                .PreviousExecutionIds = NKqp::SequenceToJsonString(query.State.GetPreviousExecutionIds()),
            };

            if (ScriptExecutionInfoRequired) {
                PendingScriptExecutionInfo.emplace_back(query.Path);
            } else {
                ReadyQueries.emplace_back(query.Path);
            }
        }

        ListedQueries.clear();
        ContinueScan();
    }

    void Handle(NKqp::TEvGetScriptExecutionOperationResponse::TPtr& ev) {
        Y_ABORT_UNLESS(InflightScriptExecutionInfoResolve);
        --InflightScriptExecutionInfoResolve;

        Y_ABORT_UNLESS(ev->Cookie < PendingScriptExecutionInfo.size());
        const auto& path = PendingScriptExecutionInfo[ev->Cookie];

        const auto& event = *ev->Get();
        const bool ready = event.Ready;
        const auto status = event.Status;
        if (!ready && status != Ydb::StatusIds::SUCCESS && status != Ydb::StatusIds::NOT_FOUND) {
            const auto& issues = event.Issues;
            LOG_E("Get script execution info " << ev->Sender << " failed " << status << ", issues: " << issues.ToOneLineString());
            ReplyErrorAndDie(status, NKqp::AddRootIssue(TStringBuilder() << "Failed to get last script execution info for query '" << path << "'", issues));
            return;
        }

        ResolvedQueriesCount = std::max(ResolvedQueriesCount, ev->Cookie + 1);
        LOG_D("Get script execution info " << ev->Sender << " finished " << status << ", ready: " << ready << ", query path: " << path << ", remains #" << InflightScriptExecutionInfoResolve);

        if (ready || status != Ydb::StatusIds::NOT_FOUND) {
            const auto it = QueriesBatch.find(path);
            if (it == QueriesBatch.end()) {
                InternalError(TStringBuilder() << "Resolve script execution info for query '" << path << "' which is not in current batch");
                return;
            }

            auto& info = it->second;
            info.Issues = NKqp::SerializeIssues(event.Issues);
            info.RetryCount = event.RetryCount;

            if (!ready || status != Ydb::StatusIds::SUCCESS) {
                info.LastFailAt = event.LastFailAt;
                info.SuspendedUntil = event.SuspendedUntil;
            }

            if (const auto& metadata = event.Metadata) {
                Ydb::Query::ExecuteScriptMetadata deserializedMeta;
                metadata->UnpackTo(&deserializedMeta);

                const auto& execStats = deserializedMeta.exec_stats();
                info.Ast = execStats.query_ast();
                info.Plan = execStats.query_plan();
            }

            if (info.SuspendedUntil) {
                info.Status = "SUSPENDED";
            } else if (ready) {
                if (status == Ydb::StatusIds::SUCCESS) {
                    info.Status = "COMPLETED";
                } else if (status == Ydb::StatusIds::CANCELLED) {
                    info.Status = "STOPPED";
                } else {
                    info.Status = "FAILED";
                }
            }

            UsedSpace += info.GetSize();
        }

        if (!InflightScriptExecutionInfoResolve) {
            for (; ResolvedQueriesCount; --ResolvedQueriesCount) {
                ReadyQueries.emplace_back(PendingScriptExecutionInfo.front());
                PendingScriptExecutionInfo.pop_front();
            }

            if (!ReadyQueries.empty() && UsedSpace >= FreeSpace) {
                DrainReadyQueries();
                return;
            }
        }

        ContinueScan();
    }

private:
    void ProceedToScan() final {
        LOG_D("Proceed to scan");
        Become(&TStreamingQueriesScan::StateScan);

        if (AckReceived) {
            LOG_T("Start scan with free space: " << FreeSpace);
            ContinueScan();
        }
    }

    void ContinueScan() {
        if (HasInflightOperation) {
            return;
        }

        if (!DatabaseId) {
            HasInflightOperation = true;
            const auto& databaseFetcher = Register(NKqp::NWorkload::CreateDatabaseFetcherActor(SelfId(), DatabaseName, MakeIntrusive<NACLib::TUserToken>(BUILTIN_ACL_METADATA, TVector<NACLib::TSID>{})));
            LOG_D("Start database fetcher " << databaseFetcher);
            return;
        }

        if (!CheckedTablesExistence) {
            HasInflightOperation = true;
            const auto& checker = Register(new TStreamingQueriesTablesCheckerActor(AppData()->TenantName));
            LOG_D("Start streaming queries table existence check " << checker);
            return;
        }

        if (!ReadyQueries.empty()) {
            DrainReadyQueries();
            return;
        }

        if (FreeSpace <= 0) {
            LOG_D("Pause scan, no free space");
            return;
        }

        if (!ListedQueries.empty()) {
            HasInflightOperation = true;

            std::vector<TString> queriesPaths;
            queriesPaths.reserve(ListedQueries.size());
            for (const auto& query : ListedQueries) {
                queriesPaths.push_back(query.Path);
            }

            const auto& describer = Register(new TStreamingQueryDescribeActor(DatabaseName, std::move(queriesPaths), UserToken));
            LOG_D("Start streaming query describer " << describer);
            return;
        }

        if (!PendingScriptExecutionInfo.empty() || InflightScriptExecutionInfoResolve) {
            ResolveScriptExecutionInfo();
            return;
        }

        if (!ListStreamingQueriesFinished) {
            ListStreamingQueries();
            return;
        }

        // Finish scan
        DrainReadyQueries();
    }

    void ResolveScriptExecutionInfo() {
        if (InflightScriptExecutionInfoResolve) {
            return;
        }

        const auto& kqpProxyId = NKqp::MakeKqpProxyID(SelfId().NodeId());
        for (ui64 i = 0; InflightScriptExecutionInfoResolve < MAX_SCRIPT_EXECUTION_RESOLVE_INFLIGHT && i < PendingScriptExecutionInfo.size(); ++i) {
            const auto& path = PendingScriptExecutionInfo[i];
            const auto it = QueriesBatch.find(path);
            if (it == QueriesBatch.end()) {
                InternalError(TStringBuilder() << "Pending script execution info for query '" << path << "' which is not in current batch");
                return;
            }

            const auto& info = it->second;
            auto executionId = info.LastExecutionId;
            if (!executionId) {
                if (const auto& previousExecutionId = info.State.GetPreviousExecutionIds(); !previousExecutionId.empty()) {
                    executionId = *previousExecutionId.rbegin();
                } else {
                    UsedSpace += it->second.GetSize();
                    continue;
                }
            }

            auto event = std::make_unique<NKqp::TEvGetScriptExecutionOperation>(DatabaseName, NKqp::OperationIdFromExecutionId(executionId), BUILTIN_ACL_METADATA);
            event->CheckLeaseState = false;
            Send(kqpProxyId, std::move(event), 0, i);

            LOG_D("Resolving script execution info for query '" << path << "', execution id: " << executionId);
            InflightScriptExecutionInfoResolve++;
        }

        if (!InflightScriptExecutionInfoResolve) {
            ReadyQueries.insert(ReadyQueries.end(), PendingScriptExecutionInfo.begin(), PendingScriptExecutionInfo.end());
            PendingScriptExecutionInfo.clear();
        }

        ContinueScan();
    }

    void DrainReadyQueries() {
        LOG_D("Sending #" << ReadyQueries.size() << " ready queries to CA");

        auto batch = MakeHolder<NKqp::TEvKqpCompute::TEvScanData>(ScanId);
        batch->Finished = ListStreamingQueriesFinished && PendingScriptExecutionInfo.empty();
        for (const auto& path : ReadyQueries) {
            const auto it = QueriesBatch.find(path);
            if (it == QueriesBatch.end()) {
                InternalError(TStringBuilder() << "Ready query " << path << " not found in current batch");
                return;
            }

            UsedSpace = UsedSpace >= it->second.GetSize() ? UsedSpace - it->second.GetSize() : 0;

            TVector<TCell> cells;
            cells.reserve(Columns.size());
            for (const auto& column : Columns) {
                const auto extractor = Extractors.find(column.Tag);
                if (extractor == Extractors.end()) {
                    cells.emplace_back();
                } else {
                    cells.emplace_back(extractor->second(*it));
                }
            }

            batch->Rows.emplace_back(TOwnedCellVec::Make(TArrayRef<const TCell>(cells)));
            QueriesBatch.erase(it);
        }

        SendBatch(std::move(batch));
        ReadyQueries.clear();
    }

    void ListStreamingQueries() {
        HasInflightOperation = true;
        TStreamingQueryFetcherActor::TSettings settings = {
            .Reverse = Reverse,
            .PageToken = ListStreamingQueriesPageToken,
            .FreeSpace = FreeSpace,
        };

        if (const auto& cells = TableRange.From.GetCells(); !cells.empty()) {
            if (const auto& cell = cells.front(); !cell.IsNull()) {
                settings.From = TStreamingQueryFetcherActor::TRangeBound{
                    .Value = cell.AsBuf(),
                    .Inclusive = TableRange.FromInclusive || TableRange.Point,
                };
            }
        }

        if (TableRange.Point) {
            settings.To = settings.From;
        } else if (const auto& cells = TableRange.To.GetCells(); !cells.empty()) {
            if (const auto& cell = cells.back(); !cell.IsNull()) {
                settings.To = TStreamingQueryFetcherActor::TRangeBound{
                    .Value = cell.AsBuf(),
                    .Inclusive = TableRange.ToInclusive,
                };
            }
        }

        const auto& fetcher = Register(new TStreamingQueryFetcherActor::TRetry(SelfId(), DatabaseId, settings));
        LOG_D("Start streaming query fetcher " << fetcher);
    }

private:
    void InternalError(const TString& message) {
        LOG_E(message);
        ReplyErrorAndDie(Ydb::StatusIds::INTERNAL_ERROR, message);
    }

    static TString StatusToString(NKikimrKqp::TStreamingQueryState::EStatus status) {
        switch (status) {
            case NKikimrKqp::TStreamingQueryState::STATUS_UNSPECIFIED:
                return "UNSPECIFIED";
            case NKikimrKqp::TStreamingQueryState::STATUS_CREATING:
                return "CREATING";
            case NKikimrKqp::TStreamingQueryState::STATUS_CREATED:
                return "CREATED";
            case NKikimrKqp::TStreamingQueryState::STATUS_STARTING:
                return "STARTING";
            case NKikimrKqp::TStreamingQueryState::STATUS_RUNNING:
                return "RUNNING";
            case NKikimrKqp::TStreamingQueryState::STATUS_STOPPING:
                return "STOPPING";
            case NKikimrKqp::TStreamingQueryState::STATUS_STOPPED:
                return "STOPPED";
            case NKikimrKqp::TStreamingQueryState::STATUS_DELETING:
                return "DELETING";
        }
    }

    TString LogPrefix() const {
        return TStringBuilder() << "[TStreamingQueriesScan] OwnerId: " << OwnerActorId << " ActorId: " << SelfId() << " ScanId: " << ScanId << " Database: " << DatabaseName << ". ";
    }

private:
    const TExtractorsMap Extractors;
    const TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    const bool Reverse = false;
    TString DatabaseId;
    bool ScriptExecutionInfoRequired = false;
    bool CheckedTablesExistence = false;
    bool HasInflightOperation = false;
    bool ListStreamingQueriesFinished = false;
    std::optional<TString> ListStreamingQueriesPageToken;
    std::vector<TEvPrivate::TEvFetchStreamingQueriesResult::TQuery> ListedQueries; // Last list queries result

    std::vector<TString> ReadyQueries;
    std::deque<TString> PendingScriptExecutionInfo;
    ui64 InflightScriptExecutionInfoResolve = 0;
    ui64 ResolvedQueriesCount = 0;
    ui64 UsedSpace = 0;
    std::unordered_map<TString, TQueryInfo> QueriesBatch;
};

} // anonymous namespace

THolder<IActor> CreateStreamingQueriesScan(const TActorId& ownerId, ui32 scanId, const TString& database,
    const NKikimrSysView::TSysViewDescription& sysViewInfo, const TTableRange& tableRange,
    const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns,
    TIntrusiveConstPtr<NACLib::TUserToken> userToken, bool reverse) {
    return MakeHolder<TStreamingQueriesScan>(ownerId, scanId, database, sysViewInfo, tableRange, columns, std::move(userToken), reverse);
}

} // namespace NKikimr::NSysView
