#include <ydb/core/statistics/events.h>
#include <ydb/core/tablet_flat/flat_row_state.h>
#include <ydb/core/tx/datashard/datashard_impl.h>
#include <ydb/core/util/count_min_sketch.h>

#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr::NDataShard {

using namespace NActors;
using namespace NTable;

class TStatisticsScan: public NTable::IScan {
public:
    explicit TStatisticsScan(TActorId replyTo)
        : Driver(nullptr)
        , ReplyTo(replyTo)
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
            CountMinSketches.emplace_back(TCountMinSketch::Create(256, 8));
        }

        return {EScan::Feed, {}};
    }

    EScan Seek(TLead& lead, ui64) noexcept override {
        lead.To(Scheme->Tags(), {}, ESeek::Lower);

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
        auto response = std::make_unique<TEvDataShard::TEvStatisticsScanResponse>();
        auto& record = response->Record;

        if (abort != EAbort::None) {
            record.SetStatus(NKikimrTxDataShard::TEvStatisticsScanResponse::ABORTED);
            TlsActivationContext->Send(new IEventHandle(ReplyTo, TActorId(), response.release()));
            return nullptr;
        }

        record.SetStatus(NKikimrTxDataShard::TEvStatisticsScanResponse::SUCCESS);
        auto tags = Scheme->Tags();
        for (size_t t = 0; t < tags.size(); ++t) {
            auto* column = record.AddColumns();
            column->SetTag(tags[t]);

            auto countMinSketch = CountMinSketches[t]->AsStringBuf();
            auto* statCMS = column->AddStatistics();
            statCMS->SetType(NKikimr::NStat::COUNT_MIN_SKETCH);
            statCMS->SetBytes(countMinSketch.Data(), countMinSketch.Size());
        }

        TlsActivationContext->Send(new IEventHandle(ReplyTo, TActorId(), response.release()));
        return nullptr;
    }

private:
    IDriver* Driver;
    TIntrusiveConstPtr<TScheme> Scheme;

    TActorId ReplyTo;

    std::vector<std::unique_ptr<TCountMinSketch>> CountMinSketches;
};

class TDataShard::TTxHandleSafeStatisticsScan : public NTabletFlatExecutor::TTransactionBase<TDataShard> {
public:
    TTxHandleSafeStatisticsScan(TDataShard* self, TEvDataShard::TEvStatisticsScanRequest::TPtr&& ev)
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
    TEvDataShard::TEvStatisticsScanRequest::TPtr Ev;
};

void TDataShard::Handle(TEvDataShard::TEvStatisticsScanRequest::TPtr& ev, const TActorContext&) {
    Execute(new TTxHandleSafeStatisticsScan(this, std::move(ev)));
}

void TDataShard::HandleSafe(TEvDataShard::TEvStatisticsScanRequest::TPtr& ev, const TActorContext&) {
    const auto& record = ev->Get()->Record;

    TPathId tablePathId = PathIdFromPathId(record.GetTablePathId());
    auto infoIt = TableInfos.find(tablePathId.LocalPathId);
    if (infoIt == TableInfos.end()) {
        auto response = std::make_unique<TEvDataShard::TEvStatisticsScanResponse>();
        response->Record.SetStatus(NKikimrTxDataShard::TEvStatisticsScanResponse::ERROR);
        Send(ev->Sender, response.release());
        return;
    }
    const auto& tableInfo = infoIt->second;

    auto scan = std::make_unique<TStatisticsScan>(ev->Sender);

    auto scanOptions = TScanOptions()
        .SetResourceBroker("statistics_scan", 20)
        .SetReadAhead(524288, 1048576)
        .SetReadPrio(TScanOptions::EReadPrio::Low);

    QueueScan(tableInfo->LocalTid, scan.release(), -1, scanOptions);
}

} // NKikimr::NDataShard
