#include "export_common.h"
#include "export_scan.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/tablet_flat/flat_row_state.h>
#include <ydb/core/tablet_flat/flat_scan_spent.h>
#include <ydb/library/actors/core/hfunc.h>

#include <util/generic/bitmap.h>
#include <util/string/builder.h>

namespace NKikimr {
namespace NDataShard {

using namespace NActors;
using namespace NExportScan;
using namespace NTable;

class TExportScan: private NActors::IActorCallback, public IActorExceptionHandler, public NTable::IScan {
    enum EStateBits {
        ES_REGISTERED = 0, // Actor is registered
        ES_INITIALIZED, // Seek(...) was called
        ES_UPLOADER_READY,
        ES_BUFFER_SENT,
        ES_NO_MORE_DATA,

        ES_COUNT,
    };

    struct TStats: public IBuffer::TStats {
        TStats()
            : IBuffer::TStats()
        {
            auto counters = GetServiceCounters(AppData()->Counters, "tablets")->GetSubgroup("subsystem", "export");

            MonRows = counters->GetCounter("Rows", true);
            MonBytesRead = counters->GetCounter("BytesRead", true);
            MonBytesSent = counters->GetCounter("BytesSent", true);
        }

        void Aggr(ui64 rows, ui64 bytesRead, ui64 bytesSent) {
            Rows += rows;
            BytesRead += bytesRead;
            BytesSent += bytesSent;

            *MonRows += rows;
            *MonBytesRead += bytesRead;
            *MonBytesSent += bytesSent;
        }

        void Aggr(const IBuffer::TStats& stats) {
            Aggr(stats.Rows, stats.BytesRead, stats.BytesSent);
        }

        TString ToString() const {
            return TStringBuilder()
                << "Stats { "
                    << " Rows: " << Rows
                    << " BytesRead: " << BytesRead
                    << " BytesSent: " << BytesSent
                << " }";
        }

    private:
        ::NMonitoring::TDynamicCounters::TCounterPtr MonRows;
        ::NMonitoring::TDynamicCounters::TCounterPtr MonBytesRead;
        ::NMonitoring::TDynamicCounters::TCounterPtr MonBytesSent;
    };

    bool IsReady() const {
        return State.Test(ES_REGISTERED) && State.Test(ES_INITIALIZED);
    }

    void MaybeReady() {
        EXPORT_LOG_I("MaybeReady"
            << ": self# " << SelfId()
            << ", uploader# " << Uploader
            << ", isRegistered# " << State.Test(ES_REGISTERED)
            << ", isInitialized# " << State.Test(ES_INITIALIZED)
            << ", isReady# " << IsReady());
        if (IsReady()) {
            EXPORT_LOG_I("MaybeReady - sending TEvReady to uploader"
                << ": self# " << SelfId()
                << ", uploader# " << Uploader);
            Send(Uploader, new TEvExportScan::TEvReady());
        }
    }

    EScan MaybeSendBuffer() {
        const bool noMoreData = State.Test(ES_NO_MORE_DATA);
        const bool bufferFilled = Buffer->IsFilled();
        const bool uploaderReady = State.Test(ES_UPLOADER_READY);
        const bool bufferSent = State.Test(ES_BUFFER_SENT);

        EXPORT_LOG_D("MaybeSendBuffer"
            << ": self# " << SelfId()
            << ", noMoreData# " << noMoreData
            << ", bufferFilled# " << bufferFilled
            << ", uploaderReady# " << uploaderReady
            << ", bufferSent# " << bufferSent);

        if (!noMoreData && !bufferFilled) {
            return EScan::Feed;
        }

        if (!uploaderReady || bufferSent) {
            EXPORT_LOG_D("MaybeSendBuffer - sleeping"
                << ": self# " << SelfId()
                << ", uploaderReady# " << uploaderReady
                << ", bufferSent# " << bufferSent);
            Spent->Alter(false);
            return EScan::Sleep;
        }

        IBuffer::TStats stats;
        THolder<IEventBase> ev{Buffer->PrepareEvent(noMoreData, stats)};

        if (!ev) {
            Success = false;
            Error = Buffer->GetError();
            EXPORT_LOG_E("MaybeSendBuffer - failed to prepare event"
                << ": self# " << SelfId()
                << ", error# " << Error);
            return EScan::Final;
        }

        EXPORT_LOG_I("MaybeSendBuffer - sending buffer to uploader"
            << ": self# " << SelfId()
            << ", uploader# " << Uploader
            << ", noMoreData# " << noMoreData
            << ", rows# " << stats.Rows
            << ", bytesRead# " << stats.BytesRead);
        Send(Uploader, std::move(ev));
        State.Set(ES_BUFFER_SENT);
        Stats->Aggr(stats);

        if (noMoreData) {
            EXPORT_LOG_I("MaybeSendBuffer - no more data, sleeping"
                << ": self# " << SelfId());
            Spent->Alter(false);
            return EScan::Sleep;
        }

        return EScan::Feed;
    }

    void Handle(TEvExportScan::TEvReset::TPtr&) {
        Y_ENSURE(IsReady());

        EXPORT_LOG_I("Handle TEvExportScan::TEvReset"
            << ": self# " << SelfId());

        Stats.Reset(new TStats);
        State.Reset(ES_UPLOADER_READY).Reset(ES_BUFFER_SENT).Reset(ES_NO_MORE_DATA);
        Spent->Alter(true);
        Driver->Touch(EScan::Reset);
    }

    void Handle(TEvExportScan::TEvFeed::TPtr&) {
        Y_ENSURE(IsReady());

        EXPORT_LOG_I("Handle TEvExportScan::TEvFeed"
            << ": self# " << SelfId()
            << ", uploader# " << Uploader);

        State.Set(ES_UPLOADER_READY).Reset(ES_BUFFER_SENT);
        Spent->Alter(true);
        if (EScan::Feed == MaybeSendBuffer()) {
            Driver->Touch(EScan::Feed);
        }
    }

    void Handle(TEvExportScan::TEvFinish::TPtr& ev) {
        Y_ENSURE(IsReady());

        EXPORT_LOG_I("Handle TEvExportScan::TEvFinish"
            << ": self# " << SelfId()
            << ", sender# " << ev->Sender
            << ", success# " << ev->Get()->Success
            << ", error# " << ev->Get()->Error
            << ", msg# " << ev->Get()->ToString());

        Success = ev->Get()->Success;
        Error = ev->Get()->Error;
        EXPORT_LOG_I("Handle TEvFinish - touching driver with Final"
            << ": self# " << SelfId());
        Driver->Touch(EScan::Final);
    }

public:
    static constexpr TStringBuf LogPrefix() {
        return "scanner"sv;
    }

    explicit TExportScan(std::function<IActor*()>&& createUploaderFn, IBuffer::TPtr buffer)
        : IActorCallback(static_cast<TReceiveFunc>(&TExportScan::StateWork), NKikimrServices::TActivity::EXPORT_SCAN_ACTOR)
        , CreateUploaderFn(std::move(createUploaderFn))
        , Buffer(std::move(buffer))
        , Stats(new TStats)
        , Driver(nullptr)
        , Success(false)
    {
    }

    void Describe(IOutputStream& o) const override {
        o << "ExportScan { "
              << "Uploader: " << Uploader
              << Stats->ToString() << " "
              << "Success: " << Success
              << "Error: " << Error
          << " }";
    }

    IScan::TInitialState Prepare(IDriver* driver, TIntrusiveConstPtr<TScheme> scheme) override {
        TlsActivationContext->AsActorContext().RegisterWithSameMailbox(this);

        Driver = driver;
        Scheme = std::move(scheme);
        Spent = new TSpent(TAppData::TimeProvider.Get());
        Buffer->ColumnsOrder(Scheme->Tags());

        return {EScan::Feed, {}};
    }

    void Registered(TActorSystem* sys, const TActorId&) override {
        EXPORT_LOG_I("Registered - creating uploader"
            << ": self# " << SelfId());
        Uploader = sys->Register(CreateUploaderFn(), TMailboxType::HTSwap, AppData()->BatchPoolId);
        EXPORT_LOG_I("Registered - uploader created"
            << ": self# " << SelfId()
            << ", uploader# " << Uploader);

        State.Set(ES_REGISTERED);
        MaybeReady();
    }

    EScan Seek(TLead& lead, ui64) override {
        EXPORT_LOG_I("Seek called"
            << ": self# " << SelfId()
            << ", uploader# " << Uploader);
        lead.To(Scheme->Tags(), {}, ESeek::Lower);
        Buffer->Clear();

        State.Set(ES_INITIALIZED);
        EXPORT_LOG_I("Seek - set initialized, calling MaybeReady"
            << ": self# " << SelfId());
        MaybeReady();

        Spent->Alter(true);
        return EScan::Feed;
    }

    EScan Feed(TArrayRef<const TCell>, const TRow& row) override {
        if (!Buffer->Collect(row)) {
            Success = false;
            Error = Buffer->GetError();
            EXPORT_LOG_E("Error read data from table: " << Error);
            return EScan::Final;
        }

        return MaybeSendBuffer();
    }

    EScan Exhausted() override {
        EXPORT_LOG_I("Exhausted - no more data"
            << ": self# " << SelfId()
            << ", uploader# " << Uploader);
        State.Set(ES_NO_MORE_DATA);
        return MaybeSendBuffer();
    }

    TAutoPtr<IDestructable> Finish(EStatus status) override {
        EXPORT_LOG_I("Finish called"
            << ": self# " << SelfId()
            << ", status# " << static_cast<int>(status)
            << ", success# " << Success
            << ", error# " << Error);
        auto outcome = EExportOutcome::Success;
        if (status != EStatus::Done) {
            outcome = status == EStatus::Exception
                ? EExportOutcome::Error
                : EExportOutcome::Aborted;
        } else if (!Success) {
            outcome = EExportOutcome::Error;
        }

        EXPORT_LOG_I("Finish - outcome determined"
            << ": self# " << SelfId()
            << ", outcome# " << static_cast<int>(outcome)
            << ", bytesRead# " << Stats->BytesRead
            << ", rows# " << Stats->Rows);
        PassAway();
        return new TExportScanProduct(outcome, Error, Stats->BytesRead, Stats->Rows);
    }

    bool OnUnhandledException(const std::exception& exc) override {
        if (!Driver) {
            return false;
        }
        Driver->Throw(exc);
        return true;
    }

    void PassAway() override {
        if (const auto& actorId = std::exchange(Uploader, {})) {
            Send(actorId, new TEvents::TEvPoisonPill());
        }

        IActorCallback::PassAway();
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvExportScan::TEvReset, Handle);
            hFunc(TEvExportScan::TEvFeed, Handle);
            hFunc(TEvExportScan::TEvFinish, Handle);
        }
    }

private:
    std::function<IActor*()> CreateUploaderFn;
    IBuffer::TPtr Buffer;

    TActorId Uploader;
    THolder<TStats> Stats;

    IDriver* Driver;
    TIntrusiveConstPtr<TScheme> Scheme;
    TAutoPtr<TSpent> Spent;

    TBitMap<EStateBits::ES_COUNT> State;
    bool Success;
    TString Error;

}; // TExportScan

NTable::IScan* CreateExportScan(IBuffer::TPtr buffer, std::function<IActor*()>&& createUploaderFn) {
    return new TExportScan(std::move(createUploaderFn), std::move(buffer));
}

} // NDataShard
} // NKikimr
