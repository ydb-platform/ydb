#include <ydb/core/statistics/aggregator/analyze_actor.h>

#include <ydb/library/query_actor/query_actor.h>
#include <ydb/core/statistics/common.h>
#include <ydb/core/statistics/events.h>
#include <util/string/vector.h>

#include <format>

namespace NKikimr::NStat {

class TSelectBuilder {
public:
    ui32 AddBuiltinAggregation(std::optional<TString> columnName, TString aggName) {
        auto column = TAggColumn{
            .Seq = static_cast<ui32>(Columns.size()),
            .ColumnName = std::move(columnName),
            .AggName = std::move(aggName),
        };
        Columns.push_back(std::move(column));
        return Columns.back().Seq;
    }

    template<typename... TArgs>
    ui32 AddUDAFAggregation(TString columnName, const TStringBuf& udafName, TArgs&&... params) {
        auto factory = AddFactory(udafName);

        // TODO: parameters escaping/binding
        TString paramsStr = Join(',', params...);

        auto column = TAggColumn{
            .Seq = static_cast<ui32>(Columns.size()),
            .ColumnName = std::move(columnName),
            .UdafFactory = factory,
            .Params = std::move(paramsStr),
        };
        Columns.push_back(std::move(column));
        return Columns.back().Seq;
    }

    TString Build(const TStringBuf& table) const {
        TStringBuilder res;
        for (const auto& [udaf, factory] : Udaf2Factory) {
            TStringBuilder paramsStr;
            for (size_t i = 0; i < factory.ParamCount; ++i) {
                if (i > 0) {
                    paramsStr << ",";
                }
                paramsStr << "$p" << i;
            }

            res << std::format(R"($f{0} = ({2}) -> {{ return AggregationFactory(
        "UDAF",
        ($item,$parent) -> {{ return Udf(StatisticsInternal::{1}Create, $parent as Depends)($item,{2}) }},
        ($state,$item,$parent) -> {{ return Udf(StatisticsInternal::{1}AddValue, $parent as Depends)($state, $item) }},
        StatisticsInternal::{1}Merge,
        StatisticsInternal::{1}Finalize,
        StatisticsInternal::{1}Serialize,
        StatisticsInternal::{1}Deserialize,
    )
}};
)",
                factory.Id, std::string_view(factory.Udaf), std::string_view(paramsStr));
        }

        res << "SELECT ";
        bool first = true;
        for (const auto& agg : Columns ) {
            if (first) {
                first = false;
            } else {
                res << ",";
            }
            if (agg.UdafFactory) {
                Y_ABORT_UNLESS(agg.ColumnName);
                res << "AGGREGATE_BY(" << agg.ColumnName
                    << "," << "$f" << *agg.UdafFactory << "(" << agg.Params << "))";
            } else {
                Y_ABORT_UNLESS(agg.AggName);
                res << *agg.AggName;
                if (agg.ColumnName) {
                    res << "(" << *agg.ColumnName << ")";
                } else {
                    res << "(*)";
                }
            }
        }

        res << " FROM `" << table << "`";
        return res;
    }

    size_t ColumnCount() const {
        return Columns.size();
    }

private:
    ui32 AddFactory(const TStringBuf& udafName) {
        // TODO: check UDAF existence, determine paramcount
        ui32 curId = Udaf2Factory.size();
        size_t paramCount = 2;
        auto [it, emplaced] = Udaf2Factory.try_emplace(udafName, curId, udafName, paramCount);
        if (emplaced) {
            it->second.Id = curId;
        }

        return it->second.Id;
    }

private:
    struct TFactory {
        TFactory(ui32 id, const TStringBuf& udaf, size_t paramCount)
        : Id(id), Udaf(udaf), ParamCount(paramCount)
        {}

        ui32 Id = 0;
        TString Udaf;
        size_t ParamCount = 0;
    };

    THashMap<TString, TFactory> Udaf2Factory;

    struct TAggColumn {
        ui32 Seq = 0;
        std::optional<TString> ColumnName;
        std::optional<TString> AggName;
        std::optional<ui32> UdafFactory;
        TString Params;
    };

    TVector<TAggColumn> Columns;
};


class TAnalyzeActor::TScanActor : public TQueryBase {
public:
    TScanActor(TActorId parent, TString Database, TString query, size_t columnCount)
        : TQueryBase(NKikimrServices::STATISTICS, {}, std::move(Database))
        , Parent(parent)
        , Query(std::move(query))
        , ColumnCount(columnCount)
    {}

    void OnRunQuery() override {
        RunStreamQuery(Query);
    }

    void OnStreamResult(NYdb::TResultSet&& resultSet) override {
        NYdb::TResultSetParser result(std::move(resultSet));
        if (!result.TryNextRow()) {
            Finish(Ydb::StatusIds::INTERNAL_ERROR, "unexpected query result");
        }
        if (result.ColumnsCount() != ColumnCount) {
            Finish(Ydb::StatusIds::INTERNAL_ERROR, "unexpected columns count");
        }

        TVector<NYdb::TValue> resultColumns;
        resultColumns.reserve(ColumnCount);
        for (size_t i = 0; i < ColumnCount; ++i) {
            resultColumns.push_back(result.GetValue(i));
        }

        auto response = std::make_unique<TEvPrivate::TEvAnalyzeScanResult>(
            std::move(resultColumns));
        Send(Parent, response.release());
        ResponseSent = true;
    }

    void OnQueryResult() override {
        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        if (status == Ydb::StatusIds::SUCCESS) {
            Y_ABORT_UNLESS(ResponseSent);
            return;
        }

        Y_ABORT_UNLESS(!ResponseSent);
        auto response = std::make_unique<TEvPrivate::TEvAnalyzeScanResult>(
            status, std::move(issues));
        Send(Parent, response.release());
        ResponseSent = true;
    }

private:
    TActorId Parent;
    TString Query;
    size_t ColumnCount;
    bool ResponseSent = false;
};

static constexpr ui64 CMS_WIDTH = 256;
static constexpr ui64 CMS_DEPTH = 8;

void TAnalyzeActor::Bootstrap() {
    Become(&TThis::StateNavigate);

    using TNavigate = NSchemeCache::TSchemeCacheNavigate;
    TNavigate::TEntry entry;
    entry.TableId = PathId;
    entry.RequestType = TNavigate::TEntry::ERequestType::ByTableId;
    entry.Operation = TNavigate::OpTable;

    auto request = std::make_unique<TNavigate>();
    request->DatabaseName = DatabaseName;
    request->ResultSet.emplace_back(entry);

    Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.release()));
}

void TAnalyzeActor::FinishWithFailure() {
    auto response = std::make_unique<TEvStatistics::TEvFinishTraversal>(/*success=*/false);
    Send(Parent, response.release());
    PassAway();
}


void TAnalyzeActor::Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
    const auto& request = *ev->Get()->Request;
    Y_ABORT_UNLESS(request.ResultSet.size() == 1);
    const NSchemeCache::TSchemeCacheNavigate::TEntry& entry = request.ResultSet.front();

    if (entry.Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
        SA_LOG_W("Navigate request failed with " << entry.Status
            << ", operationId: " << OperationId
            << ", PathId: " << PathId
            << ", DatabaseName: " << DatabaseName);
        FinishWithFailure();
        return;
    }

    // TODO: escape table path
    auto table = "/" + JoinVectorIntoString(entry.Path, "/");

    THashMap<ui32, TString> columnNames;
    for (const auto& col : entry.Columns) {
        columnNames[col.second.Id] = col.second.Name;
    }

    TSelectBuilder builder;

    // TODO: many statistics types
    CountSeq = builder.AddBuiltinAggregation({}, "count");
    if (!ColumnTags.empty()) {
        for (const auto& colTag : ColumnTags) {
            auto colIt = columnNames.find(colTag);
            if (colIt == columnNames.end()) {
                // Column probably already deleted, skip it.
                continue;
            }

            // TODO: escape column names
            Columns.emplace_back(
                colTag,
                builder.AddUDAFAggregation(colIt->second, "CountMinSketch", CMS_WIDTH, CMS_DEPTH));
        }
    } else {
        for (const auto& [tag, name] : columnNames) {
            Columns.emplace_back(
                tag,
                builder.AddUDAFAggregation(name, "CountMinSketch", CMS_WIDTH, CMS_DEPTH));
        }
    }

    Become(&TThis::StateQuery);
    auto actor = std::make_unique<TScanActor>(
        SelfId(), DatabaseName, builder.Build(table), builder.ColumnCount());
    Register(actor.release());
}

void TAnalyzeActor::Handle(TEvPrivate::TEvAnalyzeScanResult::TPtr& ev) {
    const auto& result = *ev->Get();
    if (result.Status != Ydb::StatusIds::SUCCESS) {
        FinishWithFailure();
        return;
    }

    auto response = std::make_unique<TEvStatistics::TEvAggregateStatisticsResponse>();
    auto& record = response->Record;
    for (const auto& col : Columns) {
        auto* column = record.AddColumns();
        column->SetTag(col.Tag);

        auto cmsData = NYdb::TValueParser(result.AggColumns.at(col.Seq)).GetOptionalBytes();
        if (!cmsData) {
            cmsData = std::unique_ptr<TCountMinSketch>(
                TCountMinSketch::Create(CMS_WIDTH, CMS_DEPTH))->AsStringBuf();
        }
        auto* stat = column->AddStatistics();
        stat->SetType(NKikimr::NStat::COUNT_MIN_SKETCH);
        stat->SetData(cmsData->data(), cmsData->size());
    }

    Send(Parent, response.release());
    PassAway();
}

} // NKikimr::NStat
