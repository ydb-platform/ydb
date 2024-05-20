#include "save_load_stats.h"

#include "events.h"

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
        Register(
            CreateTableCreator(
                { ".metadata", "statistics" },
                {
                    Col("owner_id", NScheme::NTypeIds::Uint64),
                    Col("local_path_id", NScheme::NTypeIds::Uint64),
                    Col("stat_type", NScheme::NTypeIds::Uint32),
                    Col("column_name", NScheme::NTypeIds::Utf8),
                    Col("data", NScheme::NTypeIds::String),
                },
                { "owner_id", "local_path_id", "stat_type", "column_name" },
                NKikimrServices::STATISTICS
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
    const std::vector<TString> ColumnNames;
    const std::vector<TString> Data;

public:
    TSaveStatisticsQuery(const TPathId& pathId, ui64 statType,
        std::vector<TString>&& columnNames, std::vector<TString>&& data)
        : NKikimr::TQueryBase(NKikimrServices::STATISTICS, {})
        , PathId(pathId)
        , StatType(statType)
        , ColumnNames(std::move(columnNames))
        , Data(std::move(data))
    {
        Y_ABORT_UNLESS(ColumnNames.size() == Data.size());
    }

    void OnRunQuery() override {
        TStringBuilder sql;
        sql << R"(
            DECLARE $owner_id AS Uint64;
            DECLARE $local_path_id AS Uint64;
            DECLARE $stat_type AS Uint32;
            DECLARE $column_names AS List<Utf8>;
            DECLARE $data AS List<String>;

            UPSERT INTO `.metadata/statistics`
                (owner_id, local_path_id, stat_type, column_name, data)
            VALUES
        )";

        for (size_t i = 0; i < Data.size(); ++i) {
            sql << " ($owner_id, $local_path_id, $stat_type, $column_names[" << i << "], $data[" << i << "])";
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
        auto& columnNames = params.AddParam("$column_names").BeginList();
        for (size_t i = 0; i < ColumnNames.size(); ++i) {
            columnNames
                .AddListItem()
                .Utf8(ColumnNames[i]);
        }
        columnNames.EndList().Build();
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
    std::vector<TString>&& columnNames, std::vector<TString>&& data)
{
    return new TSaveStatisticsQuery(pathId, statType, std::move(columnNames), std::move(data));
}


class TLoadStatisticsQuery : public NKikimr::TQueryBase {
private:
    const TPathId PathId;
    const ui64 StatType;
    const TString ColumnName;
    const ui64 Cookie;

    std::optional<TString> Data;

public:
    TLoadStatisticsQuery(const TPathId& pathId, ui64 statType, const TString& columnName, ui64 cookie)
        : NKikimr::TQueryBase(NKikimrServices::STATISTICS, {})
        , PathId(pathId)
        , StatType(statType)
        , ColumnName(columnName)
        , Cookie(cookie)
    {}

    void OnRunQuery() override {
        TString sql = R"(
            DECLARE $owner_id AS Uint64;
            DECLARE $local_path_id AS Uint64;
            DECLARE $stat_type AS Uint32;
            DECLARE $column_name AS Utf8;

            SELECT
                data
            FROM `.metadata/statistics`
            WHERE
                owner_id = $owner_id AND
                local_path_id = $local_path_id AND
                stat_type = $stat_type AND
                column_name = $column_name;
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
            .AddParam("$column_name")
                .Utf8(ColumnName)
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
    const TString& columnName, ui64 cookie)
{
    return new TLoadStatisticsQuery(pathId, statType, columnName, cookie);
}


class TDeleteStatisticsQuery : public NKikimr::TQueryBase {
private:
    const TPathId PathId;

public:
    TDeleteStatisticsQuery(const TPathId& pathId)
        : NKikimr::TQueryBase(NKikimrServices::STATISTICS, {})
        , PathId(pathId)
    {
    }

    void OnRunQuery() override {
        TString sql = R"(
            DECLARE $owner_id AS Uint64;
            DECLARE $local_path_id AS Uint64;

            DELETE FROM `.metadata/statistics`
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
