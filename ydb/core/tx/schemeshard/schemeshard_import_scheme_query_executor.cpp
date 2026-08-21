#include "schemeshard_import_scheme_query_executor.h"

#include "schemeshard_import_helpers.h"
#include "schemeshard_private.h"

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/base/table_index.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/query_data/kqp_prepared_query.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/public/lib/ydb_cli/dump/util/query_utils.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <yql/essentials/ast/yql_ast_escaping.h>
#include <yql/essentials/public/issue/yql_issue.h>

#include <library/cpp/time_provider/time_provider.h>

#include <util/generic/hash_set.h>
#include <util/stream/str.h>
#include <util/string/cast.h>
#include <util/string/split.h>

#include <re2/re2.h>

#include <limits>

using namespace NKikimr::NKqp;

namespace NKikimr::NSchemeShard {

namespace {

bool IsGlobalIndex(NKikimrSchemeOp::EIndexType type) {
    switch (type) {
    case NKikimrSchemeOp::EIndexTypeGlobal:
    case NKikimrSchemeOp::EIndexTypeGlobalAsync:
    case NKikimrSchemeOp::EIndexTypeGlobalUnique:
    case NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree:
    case NKikimrSchemeOp::EIndexTypeGlobalFulltextPlain:
    case NKikimrSchemeOp::EIndexTypeGlobalFulltextRelevance:
    case NKikimrSchemeOp::EIndexTypeGlobalJson:
    case NKikimrSchemeOp::EIndexTypeGlobalFulltextCompact:
    case NKikimrSchemeOp::EIndexTypeGlobalFulltextCompactRelevance:
    case NKikimrSchemeOp::EIndexTypeGlobalJsonCompact:
        return true;
    default:
        return false;
    }
}

struct TIndexSettings {
    TString IndexToken;
    NKikimrSchemeOp::TPartitionConfig PartitionConfig;
};

TString EscapeIdentifier(TStringBuf identifier) {
    TStringStream stream;
    NYql::EscapeArbitraryAtom(identifier, '`', &stream);
    return stream.Str();
}

bool ParseFormatterIndexSettings(TStringBuf statement, TIndexSettings& result) {
    // CREATE TABLE has already been rewritten to the destination, while formatter-generated
    // ALTER statements still name the source table. Validate the token syntactically, but do not
    // use it when folding settings into the prepared destination transaction.
    [[maybe_unused]] TString tableToken;
    TString settings;
    if (!re2::RE2::FullMatch(statement, R"re(ALTER\s+TABLE\s+(`(?:\\.|[^`])*`)\s+ALTER\s+INDEX\s+(`(?:\\.|[^`])*`|[A-Za-z_][A-Za-z0-9_]*)\s+SET\s+\(\s*([^()]*)\s*\)\s*;\s*)re",
        &tableToken, &result.IndexToken, &settings)) {
        return false;
    }

    auto* policy = result.PartitionConfig.MutablePartitioningPolicy();
    THashSet<TString> seenSettings;
    bool hasSizeSetting = false;
    bool sizeEnabled = false;
    bool hasPartitionSize = false;
    for (TStringBuf setting : StringSplitter(settings).Split(',')) {
        TString name;
        TString value;
        if (!re2::RE2::FullMatch(setting, R"re(\s*([A-Z_]+)\s*=\s*([A-Z]+|[0-9]+)\s*)re", &name, &value)
            || !seenSettings.emplace(name).second)
        {
            return false;
        }

        if (name == "AUTO_PARTITIONING_BY_SIZE") {
            hasSizeSetting = true;
            if (value == "ENABLED") {
                sizeEnabled = true;
            } else if (value == "DISABLED") {
                policy->SetSizeToSplit(0);
            } else {
                return false;
            }
        } else if (name == "AUTO_PARTITIONING_PARTITION_SIZE_MB") {
            ui64 sizeMb = 0;
            if (!TryFromString(value, sizeMb)
                || sizeMb > (std::numeric_limits<ui64>::max() >> 20))
            {
                return false;
            }
            policy->SetSizeToSplit(sizeMb << 20);
            hasPartitionSize = true;
        } else if (name == "AUTO_PARTITIONING_BY_LOAD") {
            if (value == "ENABLED") {
                policy->MutableSplitByLoadSettings()->SetEnabled(true);
            } else if (value == "DISABLED") {
                policy->MutableSplitByLoadSettings()->SetEnabled(false);
            } else {
                return false;
            }
        } else if (name == "AUTO_PARTITIONING_MIN_PARTITIONS_COUNT"
            || name == "AUTO_PARTITIONING_MAX_PARTITIONS_COUNT")
        {
            ui64 count = 0;
            if (!TryFromString(value, count)
                || count == 0
                || count > std::numeric_limits<ui32>::max())
            {
                return false;
            }
            if (name == "AUTO_PARTITIONING_MIN_PARTITIONS_COUNT") {
                policy->SetMinPartitionsCount(count);
            } else {
                policy->SetMaxPartitionsCount(count);
            }
        } else {
            return false;
        }
    }

    return !seenSettings.empty()
        && hasPartitionSize == sizeEnabled
        && (!sizeEnabled || hasSizeSetting);
}

bool FoldIndexSettings(
    NKikimrSchemeOp::TModifyScheme& create,
    const TVector<TIndexSettings>& settings)
{
    if (settings.empty()) {
        return true;
    }
    if (create.GetOperationType() != NKikimrSchemeOp::ESchemeOpCreateIndexedTable) {
        return false;
    }

    auto& indexedTable = *create.MutableCreateIndexedTable();
    THashSet<TString> alteredIndexes;
    for (const auto& indexSettings : settings) {
        bool found = false;
        for (auto& index : *indexedTable.MutableIndexDescription()) {
            if (indexSettings.IndexToken != EscapeIdentifier(index.GetName())
                && indexSettings.IndexToken != index.GetName())
            {
                continue;
            }

            const auto indexType = NTableIndex::GetIndexType(index);
            if (!IsGlobalIndex(indexType)
                || indexType == NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree)
            {
                return false;
            }
            const TVector<TString> indexKeys(index.GetKeyColumnNames().begin(), index.GetKeyColumnNames().end());
            const size_t implTableCount = NTableIndex::GetImplTables(indexType, indexKeys).size();
            auto* implTableDescriptions = index.MutableIndexImplTableDescriptions();
            if (!implTableCount
                || (!implTableDescriptions->empty()
                    && static_cast<size_t>(implTableDescriptions->size()) != implTableCount))
            {
                return false;
            }

            while (static_cast<size_t>(implTableDescriptions->size()) < implTableCount) {
                implTableDescriptions->Add();
            }
            if (!alteredIndexes.emplace(index.GetName()).second) {
                return false;
            }
            implTableDescriptions->Mutable(0)->MutablePartitionConfig()->CopyFrom(
                indexSettings.PartitionConfig);
            found = true;
            break;
        }
        if (!found) {
            return false;
        }
    }

    return true;
}

} // anonymous namespace

class TSchemeQueryExecutor: public TActorBootstrapped<TSchemeQueryExecutor> {

    bool PrepareTableQuery() {
        if (CreationQueryPathType != NKikimrSchemeOp::EPathTypeTable) {
            return true;
        }
        if (!SchemeQuery.Contains("ALTER TABLE")) {
            return true;
        }

        TVector<TString> statements;
        NYql::TIssues issues;
        if (!NYdb::NDump::SplitSqlStatements(SchemeQuery, statements, issues) || statements.empty()) {
            Finish(Ydb::StatusIds::GENERIC_ERROR, TStringBuilder()
                << "cannot split CREATE TABLE query: " << issues.ToOneLineString());
            return false;
        }
        SchemeQuery.clear();
        IndexSettings.clear();
        bool hasCreateTable = false;
        for (const auto& statement : statements) {
            TIndexSettings indexSettings;
            if (ParseFormatterIndexSettings(statement, indexSettings)) {
                if (!hasCreateTable) {
                    Finish(Ydb::StatusIds::GENERIC_ERROR,
                        "index settings precede CREATE TABLE");
                    return false;
                }
                IndexSettings.push_back(std::move(indexSettings));
                continue;
            }

            if (!IndexSettings.empty()
                || (!re2::RE2::PartialMatch(statement, R"re(^\s*PRAGMA\s+)re")
                    && !re2::RE2::PartialMatch(statement, R"re(^\s*CREATE\s+(?:TEMPORARY\s+)?TABLE\s+)re")))
            {
                Finish(Ydb::StatusIds::GENERIC_ERROR,
                    "expected CREATE TABLE followed only by formatter-generated index settings");
                return false;
            }

            if (re2::RE2::PartialMatch(statement, R"re(^\s*CREATE\s+(?:TEMPORARY\s+)?TABLE\s+)re")) {
                if (hasCreateTable) {
                    Finish(Ydb::StatusIds::GENERIC_ERROR, "expected exactly one CREATE TABLE statement");
                    return false;
                }
                hasCreateTable = true;
            }
            SchemeQuery += statement;
        }
        if (!hasCreateTable) {
            Finish(Ydb::StatusIds::GENERIC_ERROR, "CREATE TABLE statement was not found");
            return false;
        }
        return true;
    }

    std::unique_ptr<TEvKqp::TEvCompileRequest> BuildCompileRequest() {
        UserToken.Reset(MakeIntrusive<NACLib::TUserToken>(""));

        TKqpQuerySettings querySettings(NKikimrKqp::EQueryType::QUERY_TYPE_SQL_GENERIC_QUERY);
        querySettings.IsInternalCall = true;

        GUCSettings = std::make_shared<TGUCSettings>();

        TKqpQueryId query(
            TString(DefaultKikimrPublicClusterName), // cluster
            Database, // database
            "", // database id
            UserToken->GetUserSID(), // user sid
            SchemeQuery, // query text
            querySettings, // query settings
            nullptr, // query parameter types
            *GUCSettings // GUC settings
        );

        // TO DO: get default query timeout from the app config
        auto deadline = TAppData::TimeProvider->Now() + TDuration::Minutes(1);
        TKqpCounters kqpCounters(AppData()->Counters, &TlsActivationContext->AsActorContext());
        IsInterestedInResult = std::make_shared<std::atomic<bool>>(true);
        UserRequestContext.Reset(MakeIntrusive<TUserRequestContext>());

        return std::make_unique<TEvKqp::TEvCompileRequest>(
            UserToken, // user token
            "", // client address
            Nothing(), // query uid in query cache
            query, // TKqpQueryId
            false, // keep in query cache
            true, // is query action prepare?
            false, // per statement result
            deadline, // deadline
            kqpCounters.GetDbCounters(Database), // db counters
            GUCSettings, // GUC settings
            Nothing(), // application name
            IsInterestedInResult, // is still interested in result?
            UserRequestContext // user request context
        );
    }

    void PrepareSchemeQuery() {
        if (!PrepareTableQuery()) {
            return;
        }
        if (!Send(MakeKqpCompileServiceID(SelfId().NodeId()), BuildCompileRequest().release())) {
            return Finish(Ydb::StatusIds::INTERNAL_ERROR, "cannot send query request");
        }
        Become(&TThis::StateExecute);
    }

    void HandleCompileResponse(const TEvKqp::TEvCompileResponse::TPtr& ev) {
        const auto* result = ev->Get()->CompileResult.get();
        if (!result) {
            return Finish(Ydb::StatusIds::GENERIC_ERROR, "empty compile response");
        }

        LOG_D("TSchemeQueryExecutor HandleCompileResponse"
            << ", self: " << SelfId()
            << ", status: " << result->Status;
        );

        if (result->Status != Ydb::StatusIds::SUCCESS) {
            return Finish(result->Status, result->Issues.ToOneLineString());
        }
        if (!result->PreparedQuery) {
            return Finish(Ydb::StatusIds::GENERIC_ERROR, "no prepared query");
        }
        const auto& transactions = result->PreparedQuery->GetPhysicalQuery().GetTransactions();
        if (transactions.size() != 1) {
            return Finish(Ydb::StatusIds::GENERIC_ERROR, TStringBuilder()
                << "expected exactly one physical transaction, got " << transactions.size());
        }
        if (transactions[0].GetType() != NKqpProto::TKqpPhyTx::TYPE_SCHEME
            || !transactions[0].HasSchemeOperation())
        {
            return Finish(Ydb::StatusIds::GENERIC_ERROR, "expected a physical scheme transaction");
        }

        const auto& schemeOperation = transactions[0].GetSchemeOperation();
        switch (CreationQueryPathType) {
        case NKikimrSchemeOp::EPathTypeTable: {
            if (!schemeOperation.HasCreateTable()) {
                return Finish(Ydb::StatusIds::GENERIC_ERROR, "expected CREATE TABLE scheme operation");
            }

            auto createTable = schemeOperation.GetCreateTable();
            if (createTable.GetOperationType() != NKikimrSchemeOp::ESchemeOpCreateTable
                && createTable.GetOperationType() != NKikimrSchemeOp::ESchemeOpCreateIndexedTable)
            {
                return Finish(Ydb::StatusIds::GENERIC_ERROR, "expected CREATE TABLE modify scheme operation");
            }

            if (!FoldIndexSettings(createTable, IndexSettings)) {
                return Finish(Ydb::StatusIds::GENERIC_ERROR,
                    "invalid formatter-generated index settings");
            }

            return Finish(result->Status, createTable);
        }
        case NKikimrSchemeOp::EPathTypeView: {
            if (!schemeOperation.HasCreateView()) {
                return Finish(Ydb::StatusIds::GENERIC_ERROR, "expected CREATE VIEW scheme operation");
            }
            const auto& createView = schemeOperation.GetCreateView();
            return Finish(result->Status, createView);
        }
        case NKikimrSchemeOp::EPathTypeReplication: {
            if (!schemeOperation.HasCreateReplication()) {
                return Finish(Ydb::StatusIds::GENERIC_ERROR, "expected CREATE ASYNC REPLICATION scheme operation");
            }
            const auto& createReplication = schemeOperation.GetCreateReplication();
            return Finish(result->Status, createReplication);
        }
        case NKikimrSchemeOp::EPathTypeTransfer: {
            if (!schemeOperation.HasCreateTransfer()) {
                return Finish(Ydb::StatusIds::GENERIC_ERROR, "expected CREATE TRANSFER scheme operation");
            }
            const auto& createTransfer = schemeOperation.GetCreateTransfer();
            return Finish(result->Status, createTransfer);
        }
        case NKikimrSchemeOp::EPathTypeExternalDataSource: {
            if (!schemeOperation.HasCreateExternalDataSource()) {
                return Finish(Ydb::StatusIds::GENERIC_ERROR, "expected CREATE EXTERNAL DATA SOURCE scheme operation");
            }
            const auto& createExternalDataSource = schemeOperation.GetCreateExternalDataSource();
            return Finish(result->Status, createExternalDataSource);
        }
        case NKikimrSchemeOp::EPathTypeExternalTable: {
            if (!schemeOperation.HasCreateExternalTable()) {
                return Finish(Ydb::StatusIds::GENERIC_ERROR, "expected CREATE EXTERNAL TABLE scheme operation");
            }
            const auto& createExternalTable = schemeOperation.GetCreateExternalTable();
            return Finish(result->Status, createExternalTable);
        }
        default:
            return Finish(Ydb::StatusIds::GENERIC_ERROR, TStringBuilder()
                << "unsupported create query schema type: "
                << NKikimrSchemeOp::EPathType_Name(CreationQueryPathType));
        }
    }

    void Finish(Ydb::StatusIds::StatusCode status, std::variant<TString, NKikimrSchemeOp::TModifyScheme> result) {
        auto logMessage = TStringBuilder() << "TSchemeQueryExecutor Reply"
            << ", self: " << SelfId()
            << ", status: " << status;
        LOG_I(logMessage);

        std::visit([&]<typename T>(T& value) {
            if constexpr (std::is_same_v<T, TString>) {
                logMessage << ", error: " << value;
            } else if constexpr (std::is_same_v<T, NKikimrSchemeOp::TModifyScheme>) {
                logMessage << ", prepared query: " << value.ShortDebugString().Quote();
            }
            LOG_D(logMessage);
            Send(ReplyTo, new TEvPrivate::TEvImportSchemeQueryResult(ImportId, ItemIdx, status, std::move(value)));
        }, result);

        PassAway();
    }

public:

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::IMPORT_SCHEME_QUERY_EXECUTOR;
    }

    TSchemeQueryExecutor(
        TActorId replyTo,
        ui64 importId,
        ui32 itemIdx,
        const TString& schemeQuery,
        NKikimrSchemeOp::EPathType creationQueryPathType,
        const TString& database
    )
        : ReplyTo(replyTo)
        , ImportId(importId)
        , ItemIdx(itemIdx)
        , SchemeQuery(schemeQuery)
        , CreationQueryPathType(creationQueryPathType)
        , Database(database)
    {
    }

    void Bootstrap() {
        PrepareSchemeQuery();
    }

    STATEFN(StateBase) {
        switch (ev->GetTypeRewrite()) {
            sFunc(TEvents::TEvPoisonPill, PassAway);
        }
    }

    STATEFN(StateExecute) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvKqp::TEvCompileResponse, HandleCompileResponse);
        default:
            return StateBase(ev);
        }
    }

private:

    TActorId ReplyTo;
    ui64 ImportId;
    ui32 ItemIdx;
    TString SchemeQuery;
    TVector<TIndexSettings> IndexSettings;
    NKikimrSchemeOp::EPathType CreationQueryPathType;
    TString Database;

    // The following pointer-type event arguments are necessary for constructing the compile request.
    // These pointers must remain valid until the compilation response is received.
    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    TGUCSettings::TPtr GUCSettings;
    std::shared_ptr<std::atomic<bool>> IsInterestedInResult;
    TIntrusivePtr<TUserRequestContext> UserRequestContext;

}; // TSchemeQueryExecutor

IActor* CreateSchemeQueryExecutor(
    NActors::TActorId replyTo,
    ui64 importId,
    ui32 itemIdx,
    const TString& schemeQuery,
    NKikimrSchemeOp::EPathType creationQueryPathType,
    const TString& database)
{
    return new TSchemeQueryExecutor(
        replyTo,
        importId,
        itemIdx,
        schemeQuery,
        creationQueryPathType,
        database);
}

} // NKikimr::NSchemeShard
