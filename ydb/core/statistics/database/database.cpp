#include "database.h"

#include <ydb/core/statistics/common.h>
#include <ydb/core/statistics/events.h>

#include <ydb/library/table_creator/table_creator.h>
#include <ydb/library/query_actor/query_actor.h>
#include <ydb/public/lib/scheme_types/scheme_type_id.h>

namespace NKikimr::NStat {

static constexpr TStringBuf STATISTICS_TABLE = ".metadata/_statistics";

class TStatisticsTableCreator : public TActorBootstrapped<TStatisticsTableCreator> {
public:
    explicit TStatisticsTableCreator(std::unique_ptr<NActors::IEventBase> resultEvent, const TString& database)
        : ResultEvent(std::move(resultEvent))
        , Database(database)
    {}

    void Registered(NActors::TActorSystem* sys, const NActors::TActorId& owner) override {
        NActors::TActorBootstrapped<TStatisticsTableCreator>::Registered(sys, owner);
        Owner = owner;
    }

    void Bootstrap() {
        Become(&TStatisticsTableCreator::StateFunc);

        NKikimrSchemeOp::TPartitioningPolicy partitioningPolicy;
        partitioningPolicy.SetSizeToSplit(2 << 30);

        Register(
            CreateTableCreator(
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
                Database,
                true,
                std::move(partitioningPolicy)
            )
        );
    }

private:
    static NKikimrSchemeOp::TColumnDescription Col(const TString& columnName, const char* columnType) {
        NKikimrSchemeOp::TColumnDescription desc;
        desc.SetName(columnName);
        desc.SetType(columnType);
        return desc;
    }

    static NKikimrSchemeOp::TColumnDescription Col(const TString& columnName, NScheme::TTypeId columnType) {
        return Col(columnName, NScheme::TypeName(columnType));
    }

    void Handle(TEvTableCreator::TEvCreateTableResponse::TPtr&) {
        Send(Owner, std::move(ResultEvent));
        PassAway();
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvTableCreator::TEvCreateTableResponse, Handle);
    )

private:
    std::unique_ptr<NActors::IEventBase> ResultEvent;
    const TString Database;
    NActors::TActorId Owner;
};

NActors::IActor* CreateStatisticsTableCreator(std::unique_ptr<NActors::IEventBase> event, const TString& database) {
    return new TStatisticsTableCreator(std::move(event), database);
}


class TSaveStatisticsQuery : public NKikimr::TQueryBase {
private:
    const TPathId PathId;
    const std::vector<TStatisticsItem> Items;

public:
    TSaveStatisticsQuery(
        const TString& database, const TPathId& pathId, std::vector<TStatisticsItem> items)
        : NKikimr::TQueryBase(NKikimrServices::STATISTICS, {}, database, true)
        , PathId(pathId)
        , Items(std::move(items))
    {}

    void OnRunQuery() override {
        TStringBuilder sql;
        sql << R"(
            DECLARE $owner_id AS Uint64;
            DECLARE $local_path_id AS Uint64;
            DECLARE $stat_types AS List<Uint32>;
            DECLARE $column_tags AS List<Optional<Uint32>>;
            DECLARE $data AS List<String>;

            $to_struct = ($t) -> {
                RETURN <|
                    owner_id:$owner_id,
                    local_path_id:$local_path_id,
                    stat_type:$t.0,
                    column_tag:$t.1,
                    data:$t.2,
                |>;
            };

            UPSERT INTO `.metadata/_statistics`
                (owner_id, local_path_id, stat_type, column_tag, data)
            SELECT owner_id, local_path_id, stat_type, column_tag, data FROM
            AS_TABLE(ListMap(ListZip($stat_types, $column_tags, $data), $to_struct));
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
        for (const auto& item : Items) {
            statTypes
                .AddListItem()
                .Uint32(static_cast<ui32>(item.Type));
        }
        statTypes.EndList().Build();

        auto& columnTags = params.AddParam("$column_tags").BeginList();
        for (const auto& item : Items) {
            columnTags
                .AddListItem()
                .OptionalUint32(item.ColumnTag);
        }
        columnTags.EndList().Build();

        auto& data = params.AddParam("$data").BeginList();
        for (const auto& item : Items) {
            data
                .AddListItem()
                .String(item.Data);
        }
        data.EndList().Build();

        RunDataQuery(sql, &params);
    }

    void OnQueryResult() override {
        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        Y_UNUSED(issues);
        auto response = std::make_unique<TEvStatistics::TEvSaveStatisticsQueryResponse>(
            status, std::move(issues), PathId);
        Send(Owner, response.release());
    }
};

class TSaveStatisticsRetryingQuery : public TActorBootstrapped<TSaveStatisticsRetryingQuery> {
private:
    const NActors::TActorId ReplyActorId;
    const TString Database;
    const TPathId PathId;
    const std::vector<TStatisticsItem> Items;

public:
    using TSaveRetryingQuery = TQueryRetryActor<
        TSaveStatisticsQuery, TEvStatistics::TEvSaveStatisticsQueryResponse,
        const TString&, const TPathId&, const std::vector<TStatisticsItem>&>;

    TSaveStatisticsRetryingQuery(const NActors::TActorId& replyActorId, const TString& database,
        const TPathId& pathId, std::vector<TStatisticsItem>&& items)
        : ReplyActorId(replyActorId)
        , Database(database)
        , PathId(pathId)
        , Items(std::move(items))
    {}

    void Bootstrap() {
        Register(new TSaveRetryingQuery(
            SelfId(),
            TSaveRetryingQuery::IRetryPolicy::GetExponentialBackoffPolicy(
                TSaveRetryingQuery::Retryable, TDuration::MilliSeconds(10),
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
    SA_LOG_D("[DispatchLoadStatisticsQuery] QueryId[ " << queryId
        << " ], PathId[ " << pathId << " ], " << " StatType[ " << static_cast<ui32>(statType)
        << " ], ColumnTag[ " << columnTag << " ]");

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

    using TEvReadRowsRequest = NGRpcService::TGrpcRequestNoOperationCall<Ydb::Table::ReadRowsRequest, Ydb::Table::ReadRowsResponse>;

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
                SA_LOG_W("[ReadRowsResponse] QueryId[ " << queryId << " ], RowsCount[ 0 ]");
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
            SA_LOG_E("[ReadRowsResponse] QueryId[ "
                << queryId << " ] " << NYql::IssuesFromMessageAsString(response.issues()));
            query_response->Success = false;
        }

        actorSystem->Send(replyTo, query_response.release(), 0, queryId);
    });
}


class TDeleteStatisticsQuery : public NKikimr::TQueryBase {
private:
    const TPathId PathId;

public:
    TDeleteStatisticsQuery(const TString& database, const TPathId& pathId)
        : NKikimr::TQueryBase(NKikimrServices::STATISTICS, {}, database, true)
        , PathId(pathId)
    {
    }

    void OnRunQuery() override {
        TString sql = R"(
            DECLARE $owner_id AS Uint64;
            DECLARE $local_path_id AS Uint64;

            DELETE FROM `.metadata/_statistics`
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

    void OnQueryResult() override {
        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        Y_UNUSED(issues);
        auto response = std::make_unique<TEvStatistics::TEvDeleteStatisticsQueryResponse>();
        response->Status = status;
        response->Issues = std::move(issues);
        response->Success = (status == Ydb::StatusIds::SUCCESS);
        Send(Owner, response.release());
    }
};

class TDeleteStatisticsRetryingQuery : public TActorBootstrapped<TDeleteStatisticsRetryingQuery> {
private:
    const NActors::TActorId ReplyActorId;
    const TString Database;
    const TPathId PathId;

public:
    using TDeleteRetryingQuery = TQueryRetryActor<
        TDeleteStatisticsQuery, TEvStatistics::TEvDeleteStatisticsQueryResponse,
        const TString&, const TPathId&>;

    TDeleteStatisticsRetryingQuery(const NActors::TActorId& replyActorId, const TString& database,
        const TPathId& pathId)
        : ReplyActorId(replyActorId)
        , Database(database)
        , PathId(pathId)
    {}

    void Bootstrap() {
        Register(new TDeleteRetryingQuery(
            SelfId(),
            TDeleteRetryingQuery::IRetryPolicy::GetExponentialBackoffPolicy(
                TDeleteRetryingQuery::Retryable, TDuration::MilliSeconds(10),
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

} // NKikimr::NStat
