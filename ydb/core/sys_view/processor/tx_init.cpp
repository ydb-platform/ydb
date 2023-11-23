#include "processor_impl.h"
#include <ydb/core/base/feature_flags.h>

namespace NKikimr {
namespace NSysView {

struct TSysViewProcessor::TTxInit : public TTxBase {
    explicit TTxInit(TSelf* self)
        : TTxBase(self)
    {}

    TTxType GetTxType() const override { return TXTYPE_INIT; }

    template <typename TSchema, typename TMap>
    bool LoadQueryResults(NIceDb::TNiceDb& db, TMap& results) {
        results.clear();

        auto rowset = db.Table<TSchema>().Range().Select();
        if (!rowset.IsReady()) {
            return false;
        }

        while (!rowset.EndOfSet()) {
            ui64 intervalEnd = rowset.template GetValue<typename TSchema::IntervalEnd>();
            ui32 rank = rowset.template GetValue<typename TSchema::Rank>();
            TString text = rowset.template GetValue<typename TSchema::Text>();
            TString data = rowset.template GetValue<typename TSchema::Data>();

            auto key = std::make_pair(intervalEnd, rank);
            auto& result = results[key];

            if constexpr (std::is_same<typename TMap::mapped_type, TQueryToMetrics>::value) {
                result.Text = std::move(text);
                if (data) {
                    Y_PROTOBUF_SUPPRESS_NODISCARD result.Metrics.ParseFromString(data);
                }
            } else {
                if (data) {
                    Y_PROTOBUF_SUPPRESS_NODISCARD result.ParseFromString(data);
                }
                result.SetQueryText(std::move(text));
            }

            if (!rowset.Next()) {
                return false;
            }
        }

        SVLOG_D("[" << Self->TabletID() << "] Loading results: "
            << "table# " << TSchema::TableId
            << ", result count# " << results.size());

        return true;
    };

    template <typename S>
    bool LoadPartitionResults(NIceDb::TNiceDb& db, TSelf::TResultPartitionsMap& results) {
        results.clear();
        auto rowset = db.Table<S>().Range().Select();
        if (!rowset.IsReady()) {
            return false;
        }

        while (!rowset.EndOfSet()) {
            ui64 intervalEnd = rowset.template GetValue<typename S::IntervalEnd>();
            ui32 rank = rowset.template GetValue<typename S::Rank>();
            TString data = rowset.template GetValue<typename S::Data>();

            auto key = std::make_pair(intervalEnd, rank);
            auto& result = results[key];
            if (data) {
                Y_PROTOBUF_SUPPRESS_NODISCARD result.ParseFromString(data);
            }

            if (!rowset.Next()) {
                return false;
            }
        }

        SVLOG_D("[" << Self->TabletID() << "] Loading results: "
            << "table# " << S::TableId
            << ", result count# " << results.size());

        return true;
    };


    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        SVLOG_D("[" << Self->TabletID() << "] TTxInit::Execute");

        NIceDb::TNiceDb db(txc.DB);

        { // precharge
            auto sysParamsRowset = db.Table<Schema::SysParams>().Range().Select();
            auto intervalSummariesRowset = db.Table<Schema::IntervalSummaries>().Range().Select();
            auto intervalMetricsRowset = db.Table<Schema::IntervalMetrics>().Range().Select();
            auto intervalTopsRowset = db.Table<Schema::IntervalTops>().Range().Select();
            auto nodesToRequestRowset = db.Table<Schema::NodesToRequest>().Range().Select();
            auto metricsOneMinuteRowset = db.Table<Schema::MetricsOneMinute>().Range().Select();
            auto metricsOneHourRowset = db.Table<Schema::MetricsOneHour>().Range().Select();
            auto durationOneMinuteRowset = db.Table<Schema::TopByDurationOneMinute>().Range().Select();
            auto durationOneHourRowset = db.Table<Schema::TopByDurationOneHour>().Range().Select();
            auto readBytesOneMinuteRowset = db.Table<Schema::TopByDurationOneMinute>().Range().Select();
            auto readBytesOneHourRowset = db.Table<Schema::TopByReadBytesOneHour>().Range().Select();
            auto cpuTimeOneMinuteRowset = db.Table<Schema::TopByCpuTimeOneMinute>().Range().Select();
            auto cpuTimeOneHourRowset = db.Table<Schema::TopByCpuTimeOneHour>().Range().Select();
            auto reqUnitsOneMinuteRowset = db.Table<Schema::TopByRequestUnitsOneMinute>().Range().Select();
            auto reqUnitsOneHourRowset = db.Table<Schema::TopByRequestUnitsOneHour>().Range().Select();
            auto intervalPartitionTopsRowset = db.Table<Schema::IntervalPartitionTops>().Range().Select();
            auto topPartitionsOneMinuteRowset = db.Table<Schema::TopPartitionsOneMinute>().Range().Select();
            auto topPartitionsOneHourRowset = db.Table<Schema::TopPartitionsOneHour>().Range().Select();

            if (!sysParamsRowset.IsReady() ||
                !intervalSummariesRowset.IsReady() ||
                !intervalMetricsRowset.IsReady() ||
                !intervalTopsRowset.IsReady() ||
                !nodesToRequestRowset.IsReady() ||
                !metricsOneMinuteRowset.IsReady() ||
                !metricsOneHourRowset.IsReady() ||
                !durationOneMinuteRowset.IsReady() ||
                !durationOneHourRowset.IsReady() ||
                !readBytesOneMinuteRowset.IsReady() ||
                !readBytesOneHourRowset.IsReady() ||
                !cpuTimeOneMinuteRowset.IsReady() ||
                !cpuTimeOneHourRowset.IsReady() ||
                !reqUnitsOneMinuteRowset.IsReady() ||
                !reqUnitsOneHourRowset.IsReady() ||
                !intervalPartitionTopsRowset.IsReady() ||
                !topPartitionsOneMinuteRowset.IsReady() ||
                !topPartitionsOneHourRowset.IsReady())
            {
                return false;
            }
        }

        // SysParams
        {
            auto rowset = db.Table<Schema::SysParams>().Range().Select();
            if (!rowset.IsReady()) {
                return false;
            }

            while (!rowset.EndOfSet()) {
                ui64 id = rowset.GetValue<Schema::SysParams::Id>();
                TString value = rowset.GetValue<Schema::SysParams::Value>();

                switch (id) {
                    case Schema::SysParam_Database:
                        Self->Database = value;
                        SVLOG_D("[" << Self->TabletID() << "] Loading database: " << Self->Database);
                        break;
                    case Schema::SysParam_CurrentStage: {
                        auto stage = FromString<ui64>(value);
                        Self->CurrentStage = static_cast<EStage>(stage);
                        SVLOG_D("[" << Self->TabletID() << "] Loading stage: " << stage);
                        break;
                    }
                    case Schema::SysParam_IntervalEnd:
                        Self->IntervalEnd = TInstant::MicroSeconds(FromString<ui64>(value));
                        SVLOG_D("[" << Self->TabletID() << "] Loading interval end: " << Self->IntervalEnd);
                        break;
                    default:
                        SVLOG_CRIT("[" << Self->TabletID() << "] Unexpected SysParam id: " << id);
                }

                if (!rowset.Next()) {
                    return false;
                }
            }
        }

        // IntervalSummaries
        {
            Self->Queries.clear();
            Self->ByCpu.clear();
            Self->SummaryNodes.clear();

            auto rowset = db.Table<Schema::IntervalSummaries>().Range().Select();
            if (!rowset.IsReady()) {
                return false;
            }

            size_t totalNodeIdsCount = 0;
            while (!rowset.EndOfSet()) {
                TQueryHash queryHash = rowset.GetValue<Schema::IntervalSummaries::QueryHash>();
                ui64 cpu = rowset.GetValue<Schema::IntervalSummaries::CPU>();
                TNodeId nodeId = rowset.GetValue<Schema::IntervalSummaries::NodeId>();

                auto& query = Self->Queries[queryHash];
                query.Cpu += cpu;
                query.Nodes.emplace_back(nodeId, cpu);
                ++totalNodeIdsCount;

                Self->SummaryNodes.insert(nodeId);

                if (!rowset.Next()) {
                    return false;
                }
            }

            for (const auto& [queryHash, query] : Self->Queries) {
                Self->ByCpu.emplace(query.Cpu, queryHash);
            }

            SVLOG_D("[" << Self->TabletID() << "] Loading interval summaries: "
                << "query count# " << Self->Queries.size()
                << ", node ids count# " << Self->SummaryNodes.size()
                << ", total count# " << totalNodeIdsCount);
        }

        // IntervalMetrics
        {
            Self->QueryMetrics.clear();

            auto rowset = db.Table<Schema::IntervalMetrics>().Range().Select();
            if (!rowset.IsReady()) {
                return false;
            }
            while (!rowset.EndOfSet()) {
                TQueryHash queryHash = rowset.GetValue<Schema::IntervalMetrics::QueryHash>();
                TString metrics = rowset.GetValue<Schema::IntervalMetrics::Metrics>();
                TString text = rowset.GetValue<Schema::IntervalMetrics::Text>();

                auto& queryMetrics = Self->QueryMetrics[queryHash];
                queryMetrics.Text = text;
                if (metrics) {
                    Y_PROTOBUF_SUPPRESS_NODISCARD queryMetrics.Metrics.ParseFromString(metrics);
                }

                if (!rowset.Next()) {
                    return false;
                }
            }
            SVLOG_D("[" << Self->TabletID() << "] Loading interval metrics: "
                << "query count# " << Self->QueryMetrics.size());
        }

        // IntervalTops
        {
            Self->ByDurationMinute.clear();
            Self->ByDurationHour.clear();
            Self->ByReadBytesMinute.clear();
            Self->ByReadBytesHour.clear();
            Self->ByCpuTimeMinute.clear();
            Self->ByCpuTimeHour.clear();
            Self->ByRequestUnitsMinute.clear();
            Self->ByRequestUnitsHour.clear();

            auto rowset = db.Table<Schema::IntervalTops>().Range().Select();
            if (!rowset.IsReady()) {
                return false;
            }

            size_t queryCount = 0;
            while (!rowset.EndOfSet()) {
                ui32 type = rowset.GetValue<Schema::IntervalTops::TypeCol>();
                TQueryHash queryHash = rowset.GetValue<Schema::IntervalTops::QueryHash>();
                ui64 value = rowset.GetValue<Schema::IntervalTops::Value>();
                TNodeId nodeId = rowset.GetValue<Schema::IntervalTops::NodeId>();
                TString stats = rowset.GetValue<Schema::IntervalTops::Stats>();

                TTopQuery query{queryHash, value, nodeId, {}};
                if (stats) {
                    query.Stats = MakeHolder<NKikimrSysView::TQueryStats>();
                    Y_PROTOBUF_SUPPRESS_NODISCARD query.Stats->ParseFromString(stats);
                }

                switch ((NKikimrSysView::EStatsType)type) {
                    case NKikimrSysView::TOP_DURATION_ONE_MINUTE:
                        Self->ByDurationMinute.emplace_back(std::move(query));
                        break;
                    case NKikimrSysView::TOP_DURATION_ONE_HOUR:
                        Self->ByDurationHour.emplace_back(std::move(query));
                        break;
                    case NKikimrSysView::TOP_READ_BYTES_ONE_MINUTE:
                        Self->ByReadBytesMinute.emplace_back(std::move(query));
                        break;
                    case NKikimrSysView::TOP_READ_BYTES_ONE_HOUR:
                        Self->ByReadBytesHour.emplace_back(std::move(query));
                        break;
                    case NKikimrSysView::TOP_CPU_TIME_ONE_MINUTE:
                        Self->ByCpuTimeMinute.emplace_back(std::move(query));
                        break;
                    case NKikimrSysView::TOP_CPU_TIME_ONE_HOUR:
                        Self->ByCpuTimeHour.emplace_back(std::move(query));
                        break;
                    case NKikimrSysView::TOP_REQUEST_UNITS_ONE_MINUTE:
                        Self->ByRequestUnitsMinute.emplace_back(std::move(query));
                        break;
                    case NKikimrSysView::TOP_REQUEST_UNITS_ONE_HOUR:
                        Self->ByRequestUnitsHour.emplace_back(std::move(query));
                        break;
                    default:
                        SVLOG_CRIT("[" << Self->TabletID() << "] ignoring unexpected query stats type: " << type);
                }

                ++queryCount;
                if (!rowset.Next()) {
                    return false;
                }
            }

            std::sort(Self->ByDurationMinute.begin(), Self->ByDurationMinute.end(), TopQueryCompare);
            std::sort(Self->ByDurationHour.begin(), Self->ByDurationHour.end(), TopQueryCompare);
            std::sort(Self->ByReadBytesMinute.begin(), Self->ByReadBytesMinute.end(), TopQueryCompare);
            std::sort(Self->ByReadBytesHour.begin(), Self->ByReadBytesHour.end(), TopQueryCompare);
            std::sort(Self->ByCpuTimeMinute.begin(), Self->ByCpuTimeMinute.end(), TopQueryCompare);
            std::sort(Self->ByCpuTimeHour.begin(), Self->ByCpuTimeHour.end(), TopQueryCompare);
            std::sort(Self->ByRequestUnitsMinute.begin(), Self->ByRequestUnitsMinute.end(), TopQueryCompare);
            std::sort(Self->ByRequestUnitsHour.begin(), Self->ByRequestUnitsHour.end(), TopQueryCompare);

            SVLOG_D("[" << Self->TabletID() << "] Loading interval query tops: "
                << "total query count# " << queryCount);
        }

        // NodesToRequest
        {
            Self->NodesToRequest.clear();
            Self->NodesInFlight.clear();

            auto rowset = db.Table<Schema::NodesToRequest>().Range().Select();
            if (!rowset.IsReady()) {
                return false;
            }

            size_t totalHashesCount = 0;
            while (!rowset.EndOfSet()) {
                TNodeId nodeId = rowset.GetValue<Schema::NodesToRequest::NodeId>();
                TString hashes = rowset.GetValue<Schema::NodesToRequest::QueryHashes>();
                TString textsToGet = rowset.GetValue<Schema::NodesToRequest::TextsToGet>();
                TString byDuration = rowset.GetValue<Schema::NodesToRequest::ByDuration>();
                TString byReadBytes = rowset.GetValue<Schema::NodesToRequest::ByReadBytes>();
                TString byCpuTime = rowset.GetValue<Schema::NodesToRequest::ByCpuTime>();
                TString byRequestUnits = rowset.GetValue<Schema::NodesToRequest::ByRequestUnits>();

                auto loadHashes = [&totalHashesCount] (const TString& from, THashVector& to) {
                    auto size = from.size() / sizeof(TQueryHash);
                    to.resize(size);
                    std::memcpy(to.data(), from.data(), size * sizeof(TQueryHash));
                    totalHashesCount += size;
                };

                TNodeToQueries entry;
                entry.NodeId = nodeId;
                loadHashes(hashes, entry.Hashes);
                loadHashes(textsToGet, entry.TextsToGet);
                loadHashes(byDuration, entry.ByDuration);
                loadHashes(byReadBytes, entry.ByReadBytes);
                loadHashes(byCpuTime, entry.ByCpuTime);
                loadHashes(byRequestUnits, entry.ByRequestUnits);
                Self->NodesToRequest.emplace_back(std::move(entry));

                if (!rowset.Next()) {
                    return false;
                }
            }
            SVLOG_D("[" << Self->TabletID() << "] Loading nodes to request: "
                << "nodes count# " << Self->NodesToRequest.size()
                << ", hashes count# " << totalHashesCount);
        }

        // Metrics...
        if (!LoadQueryResults<Schema::MetricsOneMinute>(db, Self->MetricsOneMinute))
            return false;
        if (!LoadQueryResults<Schema::MetricsOneHour>(db, Self->MetricsOneHour))
            return false;

        // TopBy...
        if (!LoadQueryResults<Schema::TopByDurationOneMinute>(db, Self->TopByDurationOneMinute))
            return false;
        if (!LoadQueryResults<Schema::TopByDurationOneHour>(db, Self->TopByDurationOneHour))
            return false;
        if (!LoadQueryResults<Schema::TopByReadBytesOneMinute>(db, Self->TopByReadBytesOneMinute))
            return false;
        if (!LoadQueryResults<Schema::TopByReadBytesOneHour>(db, Self->TopByReadBytesOneHour))
            return false;
        if (!LoadQueryResults<Schema::TopByCpuTimeOneMinute>(db, Self->TopByCpuTimeOneMinute))
            return false;
        if (!LoadQueryResults<Schema::TopByCpuTimeOneHour>(db, Self->TopByCpuTimeOneHour))
            return false;
        if (!LoadQueryResults<Schema::TopByRequestUnitsOneMinute>(db, Self->TopByRequestUnitsOneMinute))
            return false;
        if (!LoadQueryResults<Schema::TopByRequestUnitsOneHour>(db, Self->TopByRequestUnitsOneHour))
            return false;

        // IntervalPartitionTops
        {
            Self->PartitionTopMinute.clear();
            Self->PartitionTopMinute.reserve(TOP_PARTITIONS_COUNT);
            Self->PartitionTopHour.clear();
            Self->PartitionTopHour.reserve(TOP_PARTITIONS_COUNT);

            auto rowset = db.Table<Schema::IntervalPartitionTops>().Range().Select();
            if (!rowset.IsReady()) {
                return false;
            }

            size_t partCount = 0;
            while (!rowset.EndOfSet()) {
                ui32 type = rowset.GetValue<Schema::IntervalPartitionTops::TypeCol>();
                TString data = rowset.GetValue<Schema::IntervalPartitionTops::Data>();

                if (data) {
                    auto partition = MakeHolder<NKikimrSysView::TTopPartitionsInfo>();
                    Y_PROTOBUF_SUPPRESS_NODISCARD partition->ParseFromString(data);

                    switch ((NKikimrSysView::EStatsType)type) {
                        case NKikimrSysView::TOP_PARTITIONS_ONE_MINUTE:
                            Self->PartitionTopMinute.emplace_back(std::move(partition));
                            break;
                        case NKikimrSysView::TOP_PARTITIONS_ONE_HOUR:
                            Self->PartitionTopHour.emplace_back(std::move(partition));
                            break;
                        default:
                            SVLOG_CRIT("[" << Self->TabletID() << "] ignoring unexpected partition stats type: " << type);
                    }
                    ++partCount;
                }

                if (!rowset.Next()) {
                    return false;
                }
            }

            auto compare = [] (const auto& l, const auto& r) {
                return l->GetCPUCores() == r->GetCPUCores() ?
                    l->GetTabletId() < r->GetTabletId() : l->GetCPUCores() > r->GetCPUCores();
            };

            std::sort(Self->PartitionTopMinute.begin(), Self->PartitionTopMinute.end(), compare);
            std::sort(Self->PartitionTopHour.begin(), Self->PartitionTopHour.end(), compare);

            SVLOG_D("[" << Self->TabletID() << "] Loading interval partition tops: "
                << "partition count# " << partCount);
        }

        // TopPartitions...
        if (!LoadPartitionResults<Schema::TopPartitionsOneMinute>(db, Self->TopPartitionsOneMinute))
            return false;
        if (!LoadPartitionResults<Schema::TopPartitionsOneHour>(db, Self->TopPartitionsOneHour))
            return false;

        auto deadline = Self->IntervalEnd + Self->TotalInterval;
        if (ctx.Now() >= deadline) {
            Self->Reset(db, ctx);
        }

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        SVLOG_D("[" << Self->TabletID() << "] TTxInit::Complete");

        if (Self->CurrentStage == COLLECT) {
            Self->ScheduleAggregate();
        } else {
            Self->ScheduleCollect();
            if (!Self->NodesToRequest.empty()) {
                Self->ScheduleSendRequests();
            }
        }

        if (AppData()->FeatureFlags.GetEnableDbCounters()) {
            Self->ScheduleApplyCounters();
            Self->ScheduleApplyLabeledCounters();
            Self->SendNavigate();
        }

        Self->SignalTabletActive(ctx);
        Self->Become(&TThis::StateWork);
    }
};

NTabletFlatExecutor::ITransaction* TSysViewProcessor::CreateTxInit() {
    return new TTxInit(this);
}

} // NSysView
} // NKikimr
