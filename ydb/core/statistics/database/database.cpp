#include "database.h"

#include <ydb/core/statistics/events.h>

#include <ydb/library/table_creator/table_creator.h>
#include <ydb/library/query_actor/query_actor.h>
#include <ydb/public/lib/scheme_types/scheme_type_id.h>

#include <util/string/join.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::STATISTICS

namespace NKikimr::NStat {

// Canonical `column_tags` key for a statistic: the comma-joined ordered tag tuple for multi-column
// stats, the single decimal tag for single-column stats, or the empty string for stats with no
// column (SIMPLE/TABLE_SUMMARY). Producer (save) and consumer (load) must agree on this encoding.
static TString SerializeColumnTags(const TColumnTags& tags) {
    if (const auto* multi = tags.AsMulti()) {
        return JoinSeq(",", *multi);
    }
    if (const auto single = tags.AsSingle()) {
        return ToString(*single);
    }
    return {};
}

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
                { ".metadata", "statistics_v2" },
                {
                    Col("owner_id", NScheme::NTypeIds::Uint64),
                    Col("local_path_id", NScheme::NTypeIds::Uint64),
                    Col("stat_type", NScheme::NTypeIds::Uint32),
                    Col("column_tags", NScheme::NTypeIds::String),
                    Col("data", NScheme::NTypeIds::String),
                    Col("sampled_data", NScheme::NTypeIds::String),
                },
                { "owner_id", "local_path_id", "stat_type", "column_tags"},
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


class TSaveStatisticsQuery : public NKikimr::TQueryBase, public TQueryRetryActorMixin<TSaveStatisticsQuery, TEvStatistics::TEvSaveStatisticsQueryResponse> {
private:
    const TPathId PathId;
    std::vector<TStatisticsItem> SampledItems;
    std::vector<TStatisticsItem> FullItems;
    bool SavingSampledRows = true;

public:
    TSaveStatisticsQuery(
        const TString& database, const TPathId& pathId, std::vector<TStatisticsItem> items)
        : NKikimr::TQueryBase(NKikimrServices::STATISTICS, {}, database, true)
        , PathId(pathId)
    {
        for (auto& item : items) {
            if (item.Sampling) {
                SampledItems.push_back(std::move(item));
            } else {
                FullItems.push_back(std::move(item));
            }
        }
    }

    void OnRunQuery() override {
        if (SavingSampledRows && SampledItems.empty()) {
            SavingSampledRows = false;
        }
        const auto& items = SavingSampledRows ? SampledItems : FullItems;
        if (items.empty()) {
            Finish();
            return;
        }

        TStringBuilder sql;
        sql << R"(
            DECLARE $rows AS List<Struct<
                column_tags: String,
                data: String,
                local_path_id: Uint64,
                owner_id: Uint64,
                stat_type: Uint32
            >>;

            UPSERT INTO `)" << StatisticsTablePath << R"(`
        )";
        if (SavingSampledRows) {
            sql << R"(
                (column_tags, local_path_id, owner_id, sampled_data, stat_type)
            SELECT column_tags, local_path_id, owner_id, data AS sampled_data, stat_type
            FROM AS_TABLE($rows);
            )";
        } else {
            sql << R"(
                (column_tags, data, local_path_id, owner_id, sampled_data, stat_type)
            SELECT column_tags, data, local_path_id, owner_id, NULL AS sampled_data, stat_type
            FROM AS_TABLE($rows);
            )";
        }

        NYdb::TParamsBuilder params;
        auto& rows = params.AddParam("$rows").BeginList();
        for (const auto& item : items) {
            auto& row = rows.AddListItem().BeginStruct();
            row.AddMember("column_tags").String(SerializeColumnTags(item.ColumnTags));
            row.AddMember("local_path_id").Uint64(PathId.LocalPathId);
            row.AddMember("owner_id").Uint64(PathId.OwnerId);
            row.AddMember("stat_type").Uint32(static_cast<ui32>(item.Type));
            if (item.Sampling) {
                NKikimrStat::TSampledStatistic payload;
                *payload.MutableSampling() = *item.Sampling;
                payload.SetData(item.Data);
                row.AddMember("data").String(payload.SerializeAsString());
            } else {
                row.AddMember("data").String(item.Data);
            }
            row.EndStruct();
        }
        rows.EndList().Build();

        // Separate statements avoid multiple write effects in RBO; one transaction keeps the batch atomic.
        TTxControl txControl = TTxControl::BeginAndCommitTx();
        if (SavingSampledRows && !FullItems.empty()) {
            txControl = TTxControl::BeginTx();
        } else if (!SavingSampledRows && !SampledItems.empty()) {
            txControl = TTxControl::ContinueAndCommitTx();
        }
        RunDataQuery(sql, &params, txControl);
    }

    void OnQueryResult() override {
        if (SavingSampledRows) {
            SavingSampledRows = false;
            if (!FullItems.empty()) {
                OnRunQuery();
                return;
            }
        }
        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
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
        const TString& database, const TPathId& pathId, EStatType statType, const TColumnTags& columnTags,
        bool acceptSampledStatistics) {
    const TString serializedColumnTags = SerializeColumnTags(columnTags);
    YDB_LOG_DEBUG("[DispatchLoadStatisticsQuery]",
        {"queryId", queryId},
        {"pathId", pathId},
        {"statType", static_cast<ui32>(statType)},
        {"columnTags", serializedColumnTags});

    const auto statisticsTablePath = CanonizePath(
        TStringBuilder() << database << '/' << StatisticsTablePath);

    auto readRowsRequest = Ydb::Table::ReadRowsRequest();
    readRowsRequest.set_path(statisticsTablePath);
    readRowsRequest.add_columns("data");
    if (acceptSampledStatistics) {
        readRowsRequest.add_columns("sampled_data");
    }

    NYdb::TValueBuilder keys_builder;
    keys_builder.BeginList()
        .AddListItem()
            .BeginStruct()
                .AddMember("owner_id").Uint64(pathId.OwnerId)
                .AddMember("local_path_id").Uint64(pathId.LocalPathId)
                .AddMember("stat_type").Uint32(static_cast<ui32>(statType))
                .AddMember("column_tags").String(serializedColumnTags)
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
    rpcFuture.Subscribe([replyTo = replyToActor, queryId, actorSystem, acceptSampledStatistics](const NThreading::TFuture<Ydb::Table::ReadRowsResponse>& future) mutable {
        const auto& response = future.GetValueSync();
        auto query_response = std::make_unique<TEvStatistics::TEvLoadStatisticsQueryResponse>();
        query_response->Status = response.status();
        NYql::IssuesFromMessage(response.issues(), query_response->Issues);

        if (response.status() == Ydb::StatusIds::SUCCESS) {
            NYdb::TResultSetParser parser(response.result_set());
            const auto rowsCount = parser.RowsCount();
            Y_ABORT_UNLESS(rowsCount < 2);

            if (rowsCount == 0) {
                YDB_LOG_WARN("[ReadRowsResponse]",
                    {"queryId", queryId},
                    {"rowsCount", 0});
            }

            if (parser.TryNextRow()) {
                auto& col = parser.ColumnParser("data");
                // may be not optional from versions before fix of bug https://github.com/ydb-platform/ydb/issues/15701
                query_response->Data = col.GetKind() == NYdb::TTypeParser::ETypeKind::Optional
                    ? col.GetOptionalString()
                    : col.GetString();
                if (acceptSampledStatistics) {
                    const auto sampledData = parser.ColumnParser("sampled_data").GetOptionalString();
                    NKikimrStat::TSampledStatistic payload;
                    if (sampledData && payload.ParseFromString(*sampledData) && payload.HasData() && payload.HasSampling()
                            && payload.GetSampling().HasRequestedRate() && payload.GetSampling().HasEligibleUnits()
                            && payload.GetSampling().HasSelectedUnits() && payload.GetSampling().HasSampleRows()) {
                        query_response->Data = std::move(*payload.MutableData());
                        query_response->Sampling = std::move(*payload.MutableSampling());
                    }
                }
            }
            query_response->Success = query_response->Data.has_value();
        } else {
            YDB_LOG_ERROR("[ReadRowsResponse]",
                {"queryId", queryId},
                {"status", response.status()},
                {"issues", query_response->Issues.ToOneLineString()});
            query_response->Success = false;
        }

        actorSystem->Send(replyTo, query_response.release(), 0, queryId);
    });
}


class TDeleteStatisticsQuery : public NKikimr::TQueryBase, public TQueryRetryActorMixin<TDeleteStatisticsQuery, TEvStatistics::TEvDeleteStatisticsQueryResponse> {
private:
    const TPathId PathId;

public:
    TDeleteStatisticsQuery(const TString& database, const TPathId& pathId)
        : NKikimr::TQueryBase(NKikimrServices::STATISTICS, {}, database, true)
        , PathId(pathId)
    {
    }

    void OnRunQuery() override {
        TString sql = TStringBuilder() << R"(
            DECLARE $owner_id AS Uint64;
            DECLARE $local_path_id AS Uint64;

            DELETE FROM `)" << StatisticsTablePath << R"(`
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

} // NKikimr::NStat
