#include "processor_impl.h"

namespace NKikimr::NSysView {

struct TSysViewProcessor::TTxTopPartitions : public TTxBase {
    NKikimrSysView::TEvSendTopPartitions Record;

    TTxTopPartitions(TSelf* self, NKikimrSysView::TEvSendTopPartitions&& record)
        : TTxBase(self)
        , Record(std::move(record))
    {}

    TTxType GetTxType() const override { return TXTYPE_TOP_PARTITIONS; }

    void ProcessTop(NIceDb::TNiceDb& db, NKikimrSysView::EStatsType statsType,
        TPartitionTop& top)
    {
        TPartitionTop result;
        result.reserve(TOP_PARTITIONS_COUNT);
        std::unordered_set<ui64> seen;
        size_t index = 0;
        auto topIt = top.begin();

        auto copyNewPartition = [&] () {
            const auto& newPartition = Record.GetPartitions(index);
            auto tabletId = newPartition.GetTabletId();

            TString data;
            Y_PROTOBUF_SUPPRESS_NODISCARD newPartition.SerializeToString(&data);

            auto partition = MakeHolder<NKikimrSysView::TTopPartitionsInfo>();
            partition->CopyFrom(newPartition);
            result.emplace_back(std::move(partition));

            db.Table<Schema::IntervalPartitionTops>().Key((ui32)statsType, tabletId).Update(
                NIceDb::TUpdate<Schema::IntervalPartitionTops::Data>(data));

            seen.insert(tabletId);
            ++index;
        };

        while (result.size() < TOP_PARTITIONS_COUNT) {
            if (topIt == top.end()) {
                if (index == Record.PartitionsSize()) {
                    break;
                }
                auto tabletId = Record.GetPartitions(index).GetTabletId();
                if (seen.find(tabletId) != seen.end()) {
                    ++index;
                    continue;
                }
                copyNewPartition();
            } else {
                auto topTabletId = (*topIt)->GetTabletId();
                if (seen.find(topTabletId) != seen.end()) {
                    ++topIt;
                    continue;
                }
                if (index == Record.PartitionsSize()) {
                    result.emplace_back(std::move(*topIt++));
                    seen.insert(topTabletId);
                    continue;
                }
                auto& newPartition = Record.GetPartitions(index);
                auto tabletId = newPartition.GetTabletId();
                if (seen.find(tabletId) != seen.end()) {
                    ++index;
                    continue;
                }
                if ((*topIt)->GetCPUCores() >= newPartition.GetCPUCores()) {
                    result.emplace_back(std::move(*topIt++));
                    seen.insert(topTabletId);
                } else {
                    copyNewPartition();
                }
            }
        }

        for (; topIt != top.end(); ++topIt) {
            auto topTabletId = (*topIt)->GetTabletId();
            if (seen.find(topTabletId) != seen.end()) {
                continue;
            }
            db.Table<Schema::IntervalPartitionTops>().Key((ui32)statsType, topTabletId).Delete();
        }

        top.swap(result);
    }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        SVLOG_D("[" << Self->TabletID() << "] TTxTopPartitions::Execute: "
            << "partition count# " << Record.PartitionsSize());

        NIceDb::TNiceDb db(txc.DB);
        ProcessTop(db, NKikimrSysView::TOP_PARTITIONS_ONE_MINUTE, Self->PartitionTopMinute);
        ProcessTop(db, NKikimrSysView::TOP_PARTITIONS_ONE_HOUR, Self->PartitionTopHour);

        return true;
    }

    void Complete(const TActorContext&) override {
        SVLOG_D("[" << Self->TabletID() << "] TTxTopPartitions::Complete");
    }
};

void TSysViewProcessor::Handle(TEvSysView::TEvSendTopPartitions::TPtr& ev) {
    auto& record = ev->Get()->Record;
    auto timeUs = record.GetTimeUs();
    auto partitionIntervalEnd = IntervalEnd + TotalInterval;

    if (timeUs < IntervalEnd.MicroSeconds() || timeUs >= partitionIntervalEnd.MicroSeconds()) {
        SVLOG_W("[" << TabletID() << "] TEvSendTopPartitions, time mismath: "
            << ", partition interval end# " << partitionIntervalEnd
            << ", event time# " << TInstant::MicroSeconds(timeUs));
        return;
    }

    Execute(new TTxTopPartitions(this, std::move(record)), TActivationContext::AsActorContext());
}

} // NKikimr::NSysView
