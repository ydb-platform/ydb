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
        using TPartitionTopKey = std::pair<ui64, ui32>;

        TPartitionTop result;
        result.reserve(TOP_PARTITIONS_COUNT);
        std::unordered_set<TPartitionTopKey> seen;
        size_t index = 0;
        auto topIt = top.begin();

        auto copyNewPartition = [&] () {
            const auto& newPartition = Record.GetPartitions(index);
            const ui64 tabletId = newPartition.GetTabletId();
            const ui32 followerId = newPartition.GetFollowerId();

            TString data;
            Y_PROTOBUF_SUPPRESS_NODISCARD newPartition.SerializeToString(&data);

            auto partition = MakeHolder<NKikimrSysView::TTopPartitionsInfo>();
            partition->CopyFrom(newPartition);
            result.emplace_back(std::move(partition));

            if (followerId == 0) {
                db.Table<Schema::IntervalPartitionTops>().Key((ui32)statsType, tabletId).Update(
                    NIceDb::TUpdate<Schema::IntervalPartitionTops::Data>(data));
            } else {
                db.Table<Schema::IntervalPartitionFollowerTops>().Key((ui32)statsType, tabletId, followerId).Update(
                    NIceDb::TUpdate<Schema::IntervalPartitionFollowerTops::Data>(data));            
            }

            seen.insert({tabletId, followerId});
            ++index;
        };

        while (result.size() < TOP_PARTITIONS_COUNT) {
            if (topIt == top.end()) {
                if (index == Record.PartitionsSize()) {
                    break;
                }
                const auto& partition = Record.GetPartitions(index);
                const ui64 tabletId = partition.GetTabletId();
                const ui32 followerId = partition.GetFollowerId();
                if (seen.contains({tabletId, followerId})) {
                    ++index;
                    continue;
                }
                copyNewPartition();
            } else {
                const ui64 topTabletId = (*topIt)->GetTabletId();
                const ui32 topFollowerId = (*topIt)->GetFollowerId();
                if (seen.contains({topTabletId, topFollowerId})) {
                    ++topIt;
                    continue;
                }
                if (index == Record.PartitionsSize()) {
                    result.emplace_back(std::move(*topIt++));
                    seen.insert({topTabletId, topFollowerId});
                    continue;
                }
                const auto& newPartition = Record.GetPartitions(index);
                const ui64 tabletId = newPartition.GetTabletId();
                const ui32 followerId = newPartition.GetFollowerId();
                if (seen.contains({tabletId, followerId})) {
                    ++index;
                    continue;
                }
                if ((*topIt)->GetCPUCores() >= newPartition.GetCPUCores()) {
                    result.emplace_back(std::move(*topIt++));
                    seen.insert({topTabletId, topFollowerId});
                } else {
                    copyNewPartition();
                }
            }
        }

        for (; topIt != top.end(); ++topIt) {
            const ui64 topTabletId = (*topIt)->GetTabletId();
            const ui64 topFollowerId = (*topIt)->GetFollowerId();
            if (seen.contains({topTabletId, topFollowerId})) {
                continue;
            }

            if (topFollowerId == 0) {
                db.Table<Schema::IntervalPartitionTops>().Key((ui32)statsType, topTabletId).Delete();
            } else {
                db.Table<Schema::IntervalPartitionFollowerTops>().Key((ui32)statsType, topTabletId, topFollowerId).Delete();
            }
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

    void Complete(const TActorContext&) noexcept override {
        SVLOG_D("[" << Self->TabletID() << "] TTxTopPartitions::Complete");
    }
};

void TSysViewProcessor::Handle(TEvSysView::TEvSendTopPartitions::TPtr& ev) {
    auto& record = ev->Get()->Record;
    auto timeUs = record.GetTimeUs();
    auto partitionIntervalEnd = IntervalEnd + TotalInterval;

    SVLOG_T("TEvSysView::TEvSendTopPartitions: " << " record " << record.ShortDebugString());

    if (timeUs < IntervalEnd.MicroSeconds() || timeUs >= partitionIntervalEnd.MicroSeconds()) {
        SVLOG_W("[" << TabletID() << "] TEvSendTopPartitions, time mismath: "
            << ", partition interval end# " << partitionIntervalEnd
            << ", event time# " << TInstant::MicroSeconds(timeUs));
        return;
    }

    Execute(new TTxTopPartitions(this, std::move(record)), TActivationContext::AsActorContext());
}

} // NKikimr::NSysView
