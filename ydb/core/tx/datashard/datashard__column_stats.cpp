#include <ydb/core/statistics/events.h>
#include <ydb/core/tablet_flat/flat_row_state.h>
#include <ydb/core/tx/datashard/datashard_impl.h>
#include <ydb/library/minsketch/count_min_sketch.h>

#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr::NDataShard {

using namespace NActors;
using namespace NTable;

class TStatisticsScan: public NTable::IScan {
public:
    explicit TStatisticsScan(TActorId replyTo, ui64 cookie, ui64 shardTabletId, TSerializedCellVec&& startKey)
        : Driver(nullptr)
        , ReplyTo(replyTo)
        , Cookie(cookie)
        , ShardTabletId(shardTabletId)
        , StartKey(std::move(startKey))
    {}

    void Describe(IOutputStream& o) const noexcept override {
        o << "StatisticsScan";
    }

    IScan::TInitialState Prepare(IDriver* driver, TIntrusiveConstPtr<TScheme> scheme) noexcept override {
        Driver = driver;
        Scheme = std::move(scheme);

        auto columnCount = Scheme->Tags().size();
        CountMinSketches.reserve(columnCount);
        for (size_t i = 0; i < columnCount; ++i) {
            CountMinSketches.emplace_back(TCountMinSketch::Create());
        }

        return {EScan::Feed, {}};
    }

    EScan Seek(TLead& lead, ui64) noexcept override {
        lead.To(Scheme->Tags(), StartKey.GetCells(), ESeek::Lower);

        return EScan::Feed;
    }

    EScan Feed(TArrayRef<const TCell> key, const TRow& row) noexcept override {
        Y_UNUSED(key);
        auto rowCells = *row;
        for (size_t i = 0; i < rowCells.size(); ++i) {
            const auto& cell = rowCells[i];
            CountMinSketches[i]->Count(cell.Data(), cell.Size());
        }
        return EScan::Feed;
    }

    EScan Exhausted() noexcept override {
        return EScan::Final;
    }

    TAutoPtr<IDestructable> Finish(EAbort abort) noexcept override {
        auto response = std::make_unique<NStat::TEvStatistics::TEvStatisticsResponse>();
        auto& record = response->Record;
        record.SetShardTabletId(ShardTabletId);

        if (abort != EAbort::None) {
            record.SetStatus(NKikimrStat::TEvStatisticsResponse::STATUS_ABORTED);
            TlsActivationContext->Send(new IEventHandle(ReplyTo, TActorId(), response.release(), 0, Cookie));
            delete this;
            return nullptr;
        }

        record.SetStatus(NKikimrStat::TEvStatisticsResponse::STATUS_SUCCESS);
        auto tags = Scheme->Tags();
        for (size_t t = 0; t < tags.size(); ++t) {
            auto* column = record.AddColumns();
            column->SetTag(tags[t]);

            auto countMinSketch = CountMinSketches[t]->AsStringBuf();
            auto* statCMS = column->AddStatistics();
            statCMS->SetType(NKikimr::NStat::COUNT_MIN_SKETCH);
            statCMS->SetData(countMinSketch.Data(), countMinSketch.Size());
        }

        TlsActivationContext->Send(new IEventHandle(ReplyTo, TActorId(), response.release(), 0, Cookie));
        delete this;
        return nullptr;
    }

private:
    IDriver* Driver = nullptr;
    TIntrusiveConstPtr<TScheme> Scheme;

    TActorId ReplyTo;
    ui64 Cookie = 0;
    ui64 ShardTabletId = 0;
    TSerializedCellVec StartKey;

    std::vector<std::unique_ptr<TCountMinSketch>> CountMinSketches;
};

class TDataShard::TTxHandleSafeStatisticsScan : public NTabletFlatExecutor::TTransactionBase<TDataShard> {
public:
    TTxHandleSafeStatisticsScan(TDataShard* self, NStat::TEvStatistics::TEvStatisticsRequest::TPtr&& ev)
        : TTransactionBase(self)
        , Ev(std::move(ev))
    {}

    bool Execute(TTransactionContext&, const TActorContext& ctx) {
        Self->HandleSafe(Ev, ctx);
        return true;
    }

    void Complete(const TActorContext&) {
    }

private:
    NStat::TEvStatistics::TEvStatisticsRequest::TPtr Ev;
};

void TDataShard::Handle(NStat::TEvStatistics::TEvStatisticsRequest::TPtr& ev, const TActorContext&) {
    Execute(new TTxHandleSafeStatisticsScan(this, std::move(ev)));
}

void TDataShard::Handle(TEvPrivate::TEvStatisticsScanFinished::TPtr&, const TActorContext&) {
    StatisticsScanTableId = 0;
    StatisticsScanId = 0;
}

void TDataShard::HandleSafe(NStat::TEvStatistics::TEvStatisticsRequest::TPtr& ev, const TActorContext&) {
    const auto& record = ev->Get()->Record;

    auto response = std::make_unique<NStat::TEvStatistics::TEvStatisticsResponse>();
    response->Record.SetShardTabletId(TabletID());

    const auto& pathId = record.GetTable().GetPathId();
    if (PathOwnerId != pathId.GetOwnerId()) {
        response->Record.SetStatus(NKikimrStat::TEvStatisticsResponse::STATUS_ERROR);
        Send(ev->Sender, response.release(), 0, ev->Cookie);
        return;
    }

    auto infoIt = TableInfos.find(pathId.GetLocalId());
    if (infoIt == TableInfos.end()) {
        response->Record.SetStatus(NKikimrStat::TEvStatisticsResponse::STATUS_ERROR);
        Send(ev->Sender, response.release(), 0, ev->Cookie);
        return;
    }
    const auto& tableInfo = infoIt->second;

    TSerializedCellVec startKey(record.GetStartKey());

    if (StatisticsScanId != 0) {
        CancelScan(StatisticsScanTableId, StatisticsScanId);
    }

    auto scan = std::make_unique<TStatisticsScan>(ev->Sender, ev->Cookie, TabletID(), std::move(startKey));

    auto scanOptions = TScanOptions()
        .SetResourceBroker("statistics_scan", 20)
        .SetReadAhead(524288, 1048576)
        .SetReadPrio(TScanOptions::EReadPrio::Low);

    StatisticsScanTableId = tableInfo->LocalTid;
    StatisticsScanId = QueueScan(StatisticsScanTableId, scan.release(), -1, scanOptions);
}

} // NKikimr::NDataShard
