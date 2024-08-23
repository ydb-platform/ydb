#include "database.h"

#include <ydb/core/statistics/events.h>

#include <ydb/library/table_creator/table_creator.h>
#include <ydb/library/query_actor/query_actor.h>
#include <ydb/public/lib/scheme_types/scheme_type_id.h>

namespace NKikimr::NStat {

class TStatisticsTableCreator : public TActorBootstrapped<TStatisticsTableCreator> {
public:
    explicit TStatisticsTableCreator(std::unique_ptr<NActors::IEventBase> resultEvent)
        : ResultEvent(std::move(resultEvent))
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
    NActors::TActorId Owner;
};

NActors::IActor* CreateStatisticsTableCreator(std::unique_ptr<NActors::IEventBase> event) {
    return new TStatisticsTableCreator(std::move(event));
}


class TSaveStatisticsQuery : public NKikimr::TQueryBase {
private:
    const TPathId PathId;
    const ui64 StatType;
    const std::vector<ui32> ColumnTags;
    const std::vector<TString> Data;
public:
    TSaveStatisticsQuery(const TPathId& pathId, ui64 statType,
        std::vector<ui32>&& columnTags, std::vector<TString>&& data)
        : NKikimr::TQueryBase(NKikimrServices::STATISTICS, {}, {}, true)
        , PathId(pathId)
        , StatType(statType)
        , ColumnTags(std::move(columnTags))
        , Data(std::move(data))
    {
        Y_ABORT_UNLESS(ColumnTags.size() == Data.size());
    }

    void OnRunQuery() override {
        TStringBuilder sql;
        sql << R"(
            DECLARE $owner_id AS Uint64;
            DECLARE $local_path_id AS Uint64;
            DECLARE $stat_type AS Uint32;
            DECLARE $column_tags AS List<Uint32>;
            DECLARE $data AS List<String>;

            UPSERT INTO `.metadata/_statistics`
                (owner_id, local_path_id, stat_type, column_tag, data)
            VALUES
        )";

        for (size_t i = 0; i < Data.size(); ++i) {
            sql << " ($owner_id, $local_path_id, $stat_type, $column_tags[" << i << "], $data[" << i << "])";
            sql << (i == Data.size() - 1 ? ";" : ",");
        }

        NYdb::TParamsBuilder params;
        params
            .AddParam("$owner_id")
                .Uint64(PathId.OwnerId)
                .Build()
            .AddParam("$local_path_id")
                .Uint64(PathId.LocalPathId)
                .Build()
            .AddParam("$stat_type")
                .Uint32(StatType)
                .Build();
        auto& columnTags = params.AddParam("$column_tags").BeginList();
        for (size_t i = 0; i < ColumnTags.size(); ++i) {
            columnTags
                .AddListItem()
                .Uint32(ColumnTags[i]);
        }
        columnTags.EndList().Build();
        auto& data = params.AddParam("$data").BeginList();
        for (size_t i = 0; i < Data.size(); ++i) {
            data
                .AddListItem()
                .String(Data[i]);
        }
        data.EndList().Build();

        RunDataQuery(sql, &params);
    }

    void OnQueryResult() override {
        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        Y_UNUSED(issues);
        auto response = std::make_unique<TEvStatistics::TEvSaveStatisticsQueryResponse>();
        response->Success = (status == Ydb::StatusIds::SUCCESS);
        Send(Owner, response.release());
    }
};

NActors::IActor* CreateSaveStatisticsQuery(const TPathId& pathId, ui64 statType,
    std::vector<ui32>&& columnTags, std::vector<TString>&& data)
{
    return new TSaveStatisticsQuery(pathId, statType, std::move(columnTags), std::move(data));
}


class TLoadStatisticsQuery : public NKikimr::TQueryBase {
private:
    const TPathId PathId;
    const ui64 StatType;
    const ui32 ColumnTag;
    const ui64 Cookie;

    std::optional<TString> Data;

public:
    TLoadStatisticsQuery(const TPathId& pathId, ui64 statType, ui32 columnTag, ui64 cookie)
        : NKikimr::TQueryBase(NKikimrServices::STATISTICS, {}, {}, true)
        , PathId(pathId)
        , StatType(statType)
        , ColumnTag(columnTag)
        , Cookie(cookie)
    {}

    void OnRunQuery() override {
        TString sql = R"(
            DECLARE $owner_id AS Uint64;
            DECLARE $local_path_id AS Uint64;
            DECLARE $stat_type AS Uint32;
            DECLARE $column_tag AS Uint32;

            SELECT
                data
            FROM `.metadata/_statistics`
            WHERE
                owner_id = $owner_id AND
                local_path_id = $local_path_id AND
                stat_type = $stat_type AND
                column_tag = $column_tag;
        )";

        NYdb::TParamsBuilder params;
        params
            .AddParam("$owner_id")
                .Uint64(PathId.OwnerId)
                .Build()
            .AddParam("$local_path_id")
                .Uint64(PathId.LocalPathId)
                .Build()
            .AddParam("$stat_type")
                .Uint32(StatType)
                .Build()
            .AddParam("$column_tag")
                .Uint32(ColumnTag)
                .Build();

        RunDataQuery(sql, &params, TTxControl::BeginTx());
    }

    void OnQueryResult() override {
        if (ResultSets.size() != 1) {
            Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected read response", false);
            return;
        }
        NYdb::TResultSetParser result(ResultSets[0]);
        if (result.RowsCount() == 0) {
            Finish(Ydb::StatusIds::BAD_REQUEST, "No data", false);
            return;
        }
        result.TryNextRow();
        Data = *result.ColumnParser("data").GetOptionalString();
        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        Y_UNUSED(issues);
        auto response = std::make_unique<TEvStatistics::TEvLoadStatisticsQueryResponse>();
        response->Success = (status == Ydb::StatusIds::SUCCESS);
        response->Cookie = Cookie;
        if (response->Success) {
            response->Data = Data;
        }
        Send(Owner, response.release());
    }
};

NActors::IActor* CreateLoadStatisticsQuery(const TPathId& pathId, ui64 statType,
    ui32 columnTag, ui64 cookie)
{
    return new TLoadStatisticsQuery(pathId, statType, columnTag, cookie);
}


class TDeleteStatisticsQuery : public NKikimr::TQueryBase {
private:
    const TPathId PathId;

public:
    TDeleteStatisticsQuery(const TPathId& pathId)
        : NKikimr::TQueryBase(NKikimrServices::STATISTICS, {}, {}, true)
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
        response->Success = (status == Ydb::StatusIds::SUCCESS);
        Send(Owner, response.release());
    }
};

NActors::IActor* CreateDeleteStatisticsQuery(const TPathId& pathId)
{
    return new TDeleteStatisticsQuery(pathId);
}

} // NKikimr::NStat
