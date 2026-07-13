#include "database.h"

#include <ydb/core/statistics/events.h>

#include <ydb/library/table_creator/table_creator.h>
#include <ydb/library/query_actor/query_actor.h>
#include <ydb/public/lib/scheme_types/scheme_type_id.h>

#include <util/string/join.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::STATISTICS

namespace NKikimr::NStat {

static constexpr TStringBuf STATISTICS_TABLE = ".metadata/_statistics";
static constexpr TStringBuf MULTI_COLUMN_STATISTICS_TABLE = ".metadata/_statistics_multi";

namespace {

TString SerializeColumnTags(const std::vector<ui32>& columnTags) {
    return JoinSeq(",", columnTags);
}

using TEvReadRowsRequest = NGRpcService::TGrpcRequestNoOperationCall<Ydb::Table::ReadRowsRequest, Ydb::Table::ReadRowsResponse>;

void DispatchReadRowsRequest(
        Ydb::Table::ReadRowsRequest&& readRowsRequest, const TString& database,
        const TActorId& replyToActor, ui64 queryId) {
    auto actorSystem = TlsActivationContext->ActorSystem();
    auto rpcFuture = NRpcService::DoLocalRpc<TEvReadRowsRequest>(
        std::move(readRowsRequest), database, Nothing(), TActivationContext::ActorSystem(), true
    );
    rpcFuture.Subscribe([replyTo = replyToActor, queryId, actorSystem](const NThreading::TFuture<Ydb::Table::ReadRowsResponse>& future) mutable {
        const auto& response = future.GetValueSync();
        auto query_response = std::make_unique<TEvStatistics::TEvLoadStatisticsQueryResponse>();

        if (response.status() == Ydb::StatusIds::SUCCESS) {
            NYdb::TResultSetParser parser(response.result_set());
            const auto rowsCount = parser.RowsCount();
            Y_ABORT_UNLESS(rowsCount < 2);

            if (rowsCount == 0) {
                YDB_LOG_WARN("[ReadRowsResponse]",
                    {"queryId", queryId},
                    {"rowsCount", 0});
            }

            query_response->Success = rowsCount > 0;

            while(parser.TryNextRow()) {
                auto& col = parser.ColumnParser("data");
                // may be not optional from versions before fix of bug https://github.com/ydb-platform/ydb/issues/15701
                query_response->Data = col.GetKind() == NYdb::TTypeParser::ETypeKind::Optional
                    ? col.GetOptionalString()
                    : col.GetString();
            }
        } else {
            YDB_LOG_ERROR("[ReadRowsResponse]",
                {"queryId", queryId},
                {"issues", NYql::IssuesFromMessageAsString(response.issues())});
            query_response->Success = false;
        }

        actorSystem->Send(replyTo, query_response.release(), 0, queryId);
    });
}

} // anonymous namespace

class TStatisticsTableCreator : public NTableCreator::TMultiTableCreator {
    using TBase = NTableCreator::TMultiTableCreator;

public:
    explicit TStatisticsTableCreator(std::unique_ptr<NActors::IEventBase> resultEvent, const TString& database)
        : TBase({ GetStatisticsTableCreator(database), GetMultiColumnStatisticsTableCreator(database) })
        , ResultEvent(std::move(resultEvent))
    {}

protected:
    void OnTablesCreated(bool success, NYql::TIssues issues) override {
        Y_UNUSED(success, issues);
        Send(Owner, std::move(ResultEvent));
    }

private:
    static NActors::IActor* GetStatisticsTableCreator(const TString& database) {
        NKikimrSchemeOp::TPartitioningPolicy partitioningPolicy;
        partitioningPolicy.SetSizeToSplit(2 << 30);

        return CreateTableCreator(
            { ".metadata", "_statistics" },
            {
                Col("owner_id", NScheme::NTypeIds::Uint64),
                Col("local_path_id", NScheme::NTypeIds::Uint64),
                Col("stat_type", NScheme::NTypeIds::Uint32),
                Col("column_tag", NScheme::NTypeIds::Uint32),
                Col("data", NScheme::NTypeIds::String),
            },
            { "owner_id", "local_path_id", "stat_type", "column_tag"},
            NKikimrServices::STATISTICS,
            Nothing(),
            database,
            true,
            std::move(partitioningPolicy)
        );
    }

    static NActors::IActor* GetMultiColumnStatisticsTableCreator(const TString& database) {
        NKikimrSchemeOp::TPartitioningPolicy partitioningPolicy;
        partitioningPolicy.SetSizeToSplit(2 << 30);

        return CreateTableCreator(
            { ".metadata", "_statistics_multi" },
            {
                Col("owner_id", NScheme::NTypeIds::Uint64),
                Col("local_path_id", NScheme::NTypeIds::Uint64),
                Col("stat_type", NScheme::NTypeIds::Uint32),
                Col("column_tags", NScheme::NTypeIds::String),
                Col("data", NScheme::NTypeIds::String),
            },
            { "owner_id", "local_path_id", "stat_type", "column_tags"},
            NKikimrServices::STATISTICS,
            Nothing(),
            database,
            true,
            std::move(partitioningPolicy)
        );
    }

private:
    std::unique_ptr<NActors::IEventBase> ResultEvent;
};

NActors::IActor* CreateStatisticsTableCreator(std::unique_ptr<NActors::IEventBase> event, const TString& database) {
    return new TStatisticsTableCreator(std::move(event), database);
}


class TSaveStatisticsQuery : public NKikimr::TQueryBase, public TQueryRetryActorMixin<TSaveStatisticsQuery, TEvStatistics::TEvSaveStatisticsQueryResponse> {
private:
    const TPathId PathId;
    const std::vector<TStatisticsItem> Items;
    bool SingleColumnHandled = false;
    bool MultiColumnHandled = false;

public:
    TSaveStatisticsQuery(
        const TString& database, const TPathId& pathId, std::vector<TStatisticsItem> items)
        : NKikimr::TQueryBase(NKikimrServices::STATISTICS, {}, database, true)
        , PathId(pathId)
        , Items(std::move(items))
    {}

    void OnRunQuery() override {
        SaveNextTable();
    }

    void OnQueryResult() override {
        SaveNextTable();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        Y_UNUSED(issues);
        auto response = std::make_unique<TEvStatistics::TEvSaveStatisticsQueryResponse>(
            status, std::move(issues), PathId);
        Send(Owner, response.release());
    }

private:
    // Single-column stats live in .metadata/_statistics (keyed by one column_tag), multi-column
    // stats in .metadata/_statistics_multi (keyed by the serialized ordered column-tag tuple).
    // A batch may mix both; each table is saved by its own single-UPSERT query, run in sequence.
    void SaveNextTable() {
        if (!SingleColumnHandled) {
            SingleColumnHandled = true;
            if (RunUpsert(/* multi = */ false)) {
                return;
            }
        }
        if (!MultiColumnHandled) {
            MultiColumnHandled = true;
            if (RunUpsert(/* multi = */ true)) {
                return;
            }
        }
        Finish();
    }

    // Upserts the batch's items for one table (multi-column or single-column). Returns false
    // without dispatching a query if the batch has no items for that table.
    bool RunUpsert(bool multi) {
        const TStringBuf table = multi ? MULTI_COLUMN_STATISTICS_TABLE : STATISTICS_TABLE;
        const TStringBuf keyColumn = multi ? "column_tags" : "column_tag";
        const TStringBuf keyType = multi ? "String" : "Optional<Uint32>";

        TString sql = TStringBuilder() << R"(
            DECLARE $owner_id AS Uint64;
            DECLARE $local_path_id AS Uint64;
            DECLARE $stat_types AS List<Uint32>;
            DECLARE $keys AS List<)" << keyType << R"(>;
            DECLARE $data AS List<String>;

            $to_struct = ($t) -> {
                RETURN <|
                    owner_id:$owner_id,
                    local_path_id:$local_path_id,
                    stat_type:$t.0,
                    )" << keyColumn << R"(:$t.1,
                    data:$t.2,
                |>;
            };

            UPSERT INTO `)" << table << R"(`
                (owner_id, local_path_id, stat_type, )" << keyColumn << R"(, data)
            SELECT owner_id, local_path_id, stat_type, )" << keyColumn << R"(, data FROM
            AS_TABLE(ListMap(ListZip($stat_types, $keys, $data), $to_struct));
        )";

        NYdb::TParamsBuilder params;
        params
            .AddParam("$owner_id")
                .Uint64(PathId.OwnerId)
                .Build()
            .AddParam("$local_path_id")
                .Uint64(PathId.LocalPathId)
                .Build();

        auto& statTypes = params.AddParam("$stat_types").BeginList();
        auto& keys = params.AddParam("$keys").BeginList();
        auto& data = params.AddParam("$data").BeginList();

        bool any = false;
        for (const auto& item : Items) {
            const auto* tags = item.ColumnTags.AsMulti();
            if ((tags != nullptr) != multi) {
                continue;
            }
            statTypes.AddListItem().Uint32(static_cast<ui32>(item.Type));
            if (multi) {
                keys.AddListItem().String(SerializeColumnTags(*tags));
            } else {
                keys.AddListItem().OptionalUint32(item.ColumnTags.AsSingle());
            }
            data.AddListItem().String(item.Data);
            any = true;
        }
        if (!any) {
            return false;
        }

        statTypes.EndList().Build();
        keys.EndList().Build();
        data.EndList().Build();

        RunDataQuery(sql, &params);
        return true;
    }
};

class TSaveStatisticsRetryingQuery : public TActorBootstrapped<TSaveStatisticsRetryingQuery> {
private:
    const NActors::TActorId ReplyActorId;
    const TString Database;
    const TPathId PathId;
    const std::vector<TStatisticsItem> Items;

public:
    TSaveStatisticsRetryingQuery(const NActors::TActorId& replyActorId, const TString& database,
        const TPathId& pathId, std::vector<TStatisticsItem>&& items)
        : ReplyActorId(replyActorId)
        , Database(database)
        , PathId(pathId)
        , Items(std::move(items))
    {}

    void Bootstrap() {
        Register(TSaveStatisticsQuery::MakeRetry(
            SelfId(),
            TQueryRetryActorBase::IRetryPolicy::GetExponentialBackoffPolicy(
                TQueryRetryActorBase::Retryable, TDuration::MilliSeconds(10),
                TDuration::MilliSeconds(200), TDuration::Seconds(1),
                std::numeric_limits<size_t>::max(), TDuration::Seconds(1)),
            Database, PathId, std::move(Items)
        ));
        Become(&TSaveStatisticsRetryingQuery::StateFunc);
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvStatistics::TEvSaveStatisticsQueryResponse, Handle);
    )

    void Handle(TEvStatistics::TEvSaveStatisticsQueryResponse::TPtr& ev) {
        Send(ReplyActorId, ev->Release().Release());
        PassAway();
    }
};

NActors::IActor* CreateSaveStatisticsQuery(const NActors::TActorId& replyActorId, const TString& database,
    const TPathId& pathId, std::vector<TStatisticsItem>&& items)
{
    return new TSaveStatisticsRetryingQuery(replyActorId, database, pathId, std::move(items));
}


void DispatchLoadStatisticsQuery(
        const TActorId& replyToActor, ui64 queryId,
        const TString& database, const TPathId& pathId, EStatType statType, std::optional<ui32> columnTag) {
    YDB_LOG_DEBUG("[DispatchLoadStatisticsQuery]",
        {"queryId", queryId},
        {"pathId", pathId},
        {"statType", static_cast<ui32>(statType)},
        {"columnTag", columnTag});

    const auto statisticsTablePath = CanonizePath(
        TStringBuilder() << database << '/' << STATISTICS_TABLE);

    auto readRowsRequest = Ydb::Table::ReadRowsRequest();
    readRowsRequest.set_path(statisticsTablePath);

    NYdb::TValueBuilder keys_builder;
    keys_builder.BeginList()
        .AddListItem()
            .BeginStruct()
                .AddMember("owner_id").Uint64(pathId.OwnerId)
                .AddMember("local_path_id").Uint64(pathId.LocalPathId)
                .AddMember("stat_type").Uint32(static_cast<ui32>(statType))
                .AddMember("column_tag").OptionalUint32(columnTag)
            .EndStruct()
        .EndList();
    auto keys = keys_builder.Build();
    auto protoKeys = readRowsRequest.mutable_keys();
    *protoKeys->mutable_type() = NYdb::TProtoAccessor::GetProto(keys.GetType());
    *protoKeys->mutable_value() = NYdb::TProtoAccessor::GetProto(keys);

    DispatchReadRowsRequest(std::move(readRowsRequest), database, replyToActor, queryId);
}


class TDeleteStatisticsQuery : public NKikimr::TQueryBase, public TQueryRetryActorMixin<TDeleteStatisticsQuery, TEvStatistics::TEvDeleteStatisticsQueryResponse> {
private:
    const TPathId PathId;
    bool SingleColumnDeleted = false;
    bool MultiColumnDeleted = false;

public:
    TDeleteStatisticsQuery(const TString& database, const TPathId& pathId)
        : NKikimr::TQueryBase(NKikimrServices::STATISTICS, {}, database, true)
        , PathId(pathId)
    {
    }

    void OnRunQuery() override {
        DeleteNextTable();
    }

    void OnQueryResult() override {
        DeleteNextTable();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        Y_UNUSED(issues);
        auto response = std::make_unique<TEvStatistics::TEvDeleteStatisticsQueryResponse>();
        response->Status = status;
        response->Issues = std::move(issues);
        response->Success = (status == Ydb::StatusIds::SUCCESS);
        Send(Owner, response.release());
    }

private:
    // Deletes the table's stats from both statistics tables, one single-DELETE query per table,
    // run in sequence.
    void DeleteNextTable() {
        if (!SingleColumnDeleted) {
            SingleColumnDeleted = true;
            RunDelete(STATISTICS_TABLE);
            return;
        }
        if (!MultiColumnDeleted) {
            MultiColumnDeleted = true;
            RunDelete(MULTI_COLUMN_STATISTICS_TABLE);
            return;
        }
        Finish();
    }

    void RunDelete(TStringBuf table) {
        TString sql = TStringBuilder() << R"(
            DECLARE $owner_id AS Uint64;
            DECLARE $local_path_id AS Uint64;

            DELETE FROM `)" << table << R"(`
            WHERE
                owner_id = $owner_id AND
                local_path_id = $local_path_id;
        )";

        NYdb::TParamsBuilder params;
        params
            .AddParam("$owner_id")
                .Uint64(PathId.OwnerId)
                .Build()
            .AddParam("$local_path_id")
                .Uint64(PathId.LocalPathId)
                .Build();

        RunDataQuery(sql, &params);
    }
};

class TDeleteStatisticsRetryingQuery : public TActorBootstrapped<TDeleteStatisticsRetryingQuery> {
private:
    const NActors::TActorId ReplyActorId;
    const TString Database;
    const TPathId PathId;

public:
    TDeleteStatisticsRetryingQuery(const NActors::TActorId& replyActorId, const TString& database,
        const TPathId& pathId)
        : ReplyActorId(replyActorId)
        , Database(database)
        , PathId(pathId)
    {}

    void Bootstrap() {
        Register(TDeleteStatisticsQuery::MakeRetry(
            SelfId(),
            TQueryRetryActorBase::IRetryPolicy::GetExponentialBackoffPolicy(
                TQueryRetryActorBase::Retryable, TDuration::MilliSeconds(10),
                TDuration::MilliSeconds(200), TDuration::Seconds(1),
                std::numeric_limits<size_t>::max(), TDuration::Seconds(1)),
            Database, PathId
        ));
        Become(&TDeleteStatisticsRetryingQuery::StateFunc);
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvStatistics::TEvDeleteStatisticsQueryResponse, Handle);
    )

    void Handle(TEvStatistics::TEvDeleteStatisticsQueryResponse::TPtr& ev) {
        Send(ReplyActorId, ev->Release().Release());
        PassAway();
    }
};

NActors::IActor* CreateDeleteStatisticsQuery(const NActors::TActorId& replyActorId, const TString& database,
    const TPathId& pathId)
{
    return new TDeleteStatisticsRetryingQuery(replyActorId, database, pathId);
}


void DispatchLoadMultiColumnStatisticsQuery(
        const TActorId& replyToActor, ui64 queryId,
        const TString& database, const TPathId& pathId, EStatType statType, const std::vector<ui32>& columnTags) {
    YDB_LOG_DEBUG("[DispatchLoadMultiColumnStatisticsQuery]",
        {"queryId", queryId},
        {"pathId", pathId},
        {"statType", static_cast<ui32>(statType)},
        {"columnTags", SerializeColumnTags(columnTags)});

    const auto statisticsTablePath = CanonizePath(
        TStringBuilder() << database << '/' << MULTI_COLUMN_STATISTICS_TABLE);

    auto readRowsRequest = Ydb::Table::ReadRowsRequest();
    readRowsRequest.set_path(statisticsTablePath);

    NYdb::TValueBuilder keys_builder;
    keys_builder.BeginList()
        .AddListItem()
            .BeginStruct()
                .AddMember("owner_id").Uint64(pathId.OwnerId)
                .AddMember("local_path_id").Uint64(pathId.LocalPathId)
                .AddMember("stat_type").Uint32(static_cast<ui32>(statType))
                .AddMember("column_tags").String(SerializeColumnTags(columnTags))
            .EndStruct()
        .EndList();
    auto keys = keys_builder.Build();
    auto protoKeys = readRowsRequest.mutable_keys();
    *protoKeys->mutable_type() = NYdb::TProtoAccessor::GetProto(keys.GetType());
    *protoKeys->mutable_value() = NYdb::TProtoAccessor::GetProto(keys);

    DispatchReadRowsRequest(std::move(readRowsRequest), database, replyToActor, queryId);
}


} // NKikimr::NStat
