#include "processor_impl.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::SYSTEM_VIEWS

namespace NKikimr {
namespace NSysView {

struct TSysViewProcessor::TTxIntervalSummary : public TTxBase {
    NKikimrSysView::TEvIntervalQuerySummary Record;

    TTxIntervalSummary(TSelf* self, NKikimrSysView::TEvIntervalQuerySummary&& record)
        : TTxBase(self)
        , Record(std::move(record))
    {}

    TTxType GetTxType() const override { return TXTYPE_INTERVAL_SUMMARY; }

    void AddSummary(NIceDb::TNiceDb& db, ui64 queryHash, ui64 cpu, TNodeId nodeId) {
        if (auto queryIt = Self->Queries.find(queryHash); queryIt != Self->Queries.end()) {
            auto& query = queryIt->second;

            auto range = Self->ByCpu.equal_range(query.Cpu);
            auto it = range.first;
            for (; it != range.second; ++it) {
                if (it->second == queryHash) {
                    break;
                }
            }
            Y_ABORT_UNLESS(it != range.second);
            Self->ByCpu.erase(it);

            query.Cpu += cpu;
            Self->ByCpu.emplace(query.Cpu, queryHash);

            auto& nodes = query.Nodes;
            size_t nodeIndex = nodes.size();
            nodes.emplace_back(nodeId, cpu);

            db.Table<Schema::IntervalSummaries>().Key(queryHash, nodeIndex).Update(
                NIceDb::TUpdate<Schema::IntervalSummaries::CPU>(cpu),
                NIceDb::TUpdate<Schema::IntervalSummaries::NodeId>(nodeId));

        } else {
            if (Self->ByCpu.size() == DistinctQueriesLimit) {
                auto it = Self->ByCpu.begin();
                if (it->first >= cpu) {
                    return;
                }
                auto removeHash = it->second;
                const auto& removeQuery = Self->Queries[removeHash];
                for (ui32 i = 0; i < removeQuery.Nodes.size(); ++i) {
                    db.Table<Schema::IntervalSummaries>().Key(removeHash, i).Delete();
                }
                Self->Queries.erase(removeHash);
                Self->ByCpu.erase(it);
            }

            TQueryToNodes query{cpu, {{nodeId, cpu}}};

            Self->Queries.emplace(queryHash, std::move(query));
            Self->ByCpu.emplace(cpu, queryHash);

            db.Table<Schema::IntervalSummaries>().Key(queryHash, 0).Update(
                NIceDb::TUpdate<Schema::IntervalSummaries::CPU>(cpu),
                NIceDb::TUpdate<Schema::IntervalSummaries::NodeId>(nodeId));
        }
    }

    void ProcessTop(NIceDb::TNiceDb& db,
        TNodeId nodeId,
        NKikimrSysView::EStatsType statsType,
        const NKikimrSysView::TEvIntervalQuerySummary::TQuerySet& queries,
        TQueryTop& top)
    {
        TQueryTop result;
        std::unordered_set<TQueryHash> seenHashes;
        size_t queryIndex = 0;
        auto topIt = top.begin();

        auto copyNewQuery = [&] () {
            auto queryHash = queries.GetHashes(queryIndex);
            auto value = queries.GetValues(queryIndex);

            TTopQuery topQuery{queryHash, value, nodeId, {}};
            result.emplace_back(std::move(topQuery));

            db.Table<Schema::IntervalTops>().Key((ui32)statsType, queryHash).Update(
                NIceDb::TUpdate<Schema::IntervalTops::Value>(value),
                NIceDb::TUpdate<Schema::IntervalTops::NodeId>(nodeId));

            seenHashes.insert(queryHash);
            ++queryIndex;
        };

        while (result.size() < TOP_QUERIES_COUNT) {
            if (topIt == top.end()) {
                if (queryIndex == queries.HashesSize()) {
                    break;
                }
                auto queryHash = queries.GetHashes(queryIndex);
                if (seenHashes.find(queryHash) != seenHashes.end()) {
                    ++queryIndex;
                    continue;
                }
                copyNewQuery();
            } else {
                auto topHash = topIt->Hash;
                if (seenHashes.find(topHash) != seenHashes.end()) {
                    ++topIt;
                    continue;
                }
                if (queryIndex == queries.HashesSize()) {
                    result.emplace_back(std::move(*topIt++));
                    seenHashes.insert(topHash);
                    continue;
                }
                auto queryHash = queries.GetHashes(queryIndex);
                if (seenHashes.find(queryHash) != seenHashes.end()) {
                    ++queryIndex;
                    continue;
                }
                if (topIt->Value >= queries.GetValues(queryIndex)) {
                    result.emplace_back(std::move(*topIt++));
                    seenHashes.insert(topHash);
                } else {
                    copyNewQuery();
                }
            }
        }

        for (; topIt != top.end(); ++topIt) {
            auto topHash = topIt->Hash;
            if (seenHashes.find(topHash) != seenHashes.end()) {
                continue;
            }
            db.Table<Schema::IntervalTops>().Key((ui32)statsType, topHash).Delete();
        }

        top.swap(result);
    }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        auto nodeId = Record.GetNodeId();

        if (Record.GetMetrics().ValuesSize() != Record.GetMetrics().HashesSize() ||
            Record.GetTopByDuration().ValuesSize() != Record.GetTopByDuration().HashesSize() ||
            Record.GetTopByReadBytes().ValuesSize() != Record.GetTopByReadBytes().HashesSize() ||
            Record.GetTopByCpuTime().ValuesSize() != Record.GetTopByCpuTime().HashesSize() ||
            Record.GetTopByRequestUnits().ValuesSize() != Record.GetTopByRequestUnits().HashesSize())
        {
            YDB_LOG_WARN("TTxIntervalSummary::Execute, malformed summary: node",
                {"tabletId", Self->TabletID()},
                {"id", nodeId});
            return true;
        }

        if (Self->SummaryNodes.find(nodeId) != Self->SummaryNodes.end()) {
            YDB_LOG_WARN("TTxIntervalSummary::Execute, duplicate summary: node",
                {"tabletId", Self->TabletID()},
                {"id", nodeId});
            return true;
        }
        Self->SummaryNodes.insert(nodeId);

        const auto& metrics = Record.GetMetrics();
        auto count = metrics.HashesSize();

        YDB_LOG_DEBUG("TTxIntervalSummary::Execute: node query",
            {"tabletId", Self->TabletID()},
            {"id", nodeId},
            {"count", count});

        NIceDb::TNiceDb db(txc.DB);
        for (size_t i = 0; i < count; ++i) {
            AddSummary(db, metrics.GetHashes(i), metrics.GetValues(i), nodeId);
        }

        ProcessTop(db, nodeId, NKikimrSysView::TOP_DURATION_ONE_MINUTE,
            Record.GetTopByDuration(), Self->ByDurationMinute);
        ProcessTop(db, nodeId, NKikimrSysView::TOP_DURATION_ONE_HOUR,
            Record.GetTopByDuration(), Self->ByDurationHour);
        ProcessTop(db, nodeId, NKikimrSysView::TOP_READ_BYTES_ONE_MINUTE,
            Record.GetTopByReadBytes(), Self->ByReadBytesMinute);
        ProcessTop(db, nodeId, NKikimrSysView::TOP_READ_BYTES_ONE_HOUR,
            Record.GetTopByReadBytes(), Self->ByReadBytesHour);
        ProcessTop(db, nodeId, NKikimrSysView::TOP_CPU_TIME_ONE_MINUTE,
            Record.GetTopByCpuTime(), Self->ByCpuTimeMinute);
        ProcessTop(db, nodeId, NKikimrSysView::TOP_CPU_TIME_ONE_HOUR,
            Record.GetTopByCpuTime(), Self->ByCpuTimeHour);
        ProcessTop(db, nodeId, NKikimrSysView::TOP_REQUEST_UNITS_ONE_MINUTE,
            Record.GetTopByRequestUnits(), Self->ByRequestUnitsMinute);
        ProcessTop(db, nodeId, NKikimrSysView::TOP_REQUEST_UNITS_ONE_HOUR,
            Record.GetTopByRequestUnits(), Self->ByRequestUnitsHour);

        return true;
    }

    void Complete(const TActorContext&) override {
        YDB_LOG_DEBUG("TTxIntervalSummary::Complete",
            {"tabletId", Self->TabletID()});
    }
};

void TSysViewProcessor::Handle(TEvSysView::TEvIntervalQuerySummary::TPtr& ev) {
    auto& record = ev->Get()->Record;
    auto nodeId = record.GetNodeId();

    if (CurrentStage != COLLECT) {
        YDB_LOG_WARN("TEvIntervalQuerySummary, wrong stage: node",
            {"tabletID", TabletID()},
            {"id", nodeId});
        return;
    }

    if (record.GetIntervalEndUs() != IntervalEnd.MicroSeconds()) {
        YDB_LOG_WARN("TEvIntervalQuerySummary, time mismath: node interval event interval",
            {"tabletID", TabletID()},
            {"id", nodeId},
            {"end", IntervalEnd},
            {"recordIntervalEnd", TInstant::MicroSeconds(record.GetIntervalEndUs())});
        return;
    }

    if (record.GetDatabase() != Database) {
        YDB_LOG_WARN("TEvIntervalQuerySummary, db mismatch: node event",
            {"tabletID", TabletID()},
            {"id", nodeId},
            {"database", Database},
            {"recordDatabase", record.GetDatabase()});
        return;
    }

    Execute(new TTxIntervalSummary(this, std::move(record)), TActivationContext::AsActorContext());
}

} // NSysView
} // NKikimr
