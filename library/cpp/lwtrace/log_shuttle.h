#pragma once

#include "log.h"
#include "probe.h"

#include <library/cpp/lwtrace/protos/lwtrace.pb.h>

#include <util/system/spinlock.h>

namespace NLWTrace {
    template <class TDepot>
    class TRunLogShuttleActionExecutor;

    ////////////////////////////////////////////////////////////////////////////////

    struct THostTimeCalculator {
        double K = 0;
        ui64 B = 0;

        THostTimeCalculator() {
            TInstant now = TInstant::Now();
            ui64 tsNow = GetCycleCount();
            K = 1000000000 / NHPTimer::GetClockRate();
            B = now.NanoSeconds() - K * tsNow;
        }

        ui64 CyclesToEpochNanoseconds(ui64 cycles) const {
            return K*cycles + B;
        }

        ui64 EpochNanosecondsToCycles(ui64 ns) const {
            return (ns - B) / K;
        }
    };

    inline ui64 CyclesToEpochNanoseconds(ui64 cycles) {
        return Singleton<THostTimeCalculator>()->CyclesToEpochNanoseconds(cycles);
    }

    inline ui64 EpochNanosecondsToCycles(ui64 ns) {
        return Singleton<THostTimeCalculator>()->EpochNanosecondsToCycles(ns);
    }

    ////////////////////////////////////////////////////////////////////////////////

    template <class TDepot>
    class TLogShuttle: public IShuttle {
    private:
        using TExecutor = TRunLogShuttleActionExecutor<TDepot>;
        TTrackLog TrackLog;
        TExecutor* Executor;
        bool Ignore = false;
        size_t MaxTrackLength;
        TAdaptiveLock Lock;
        TAtomic ForkFailed = 0;

    public:
        explicit TLogShuttle(TExecutor* executor)
            : IShuttle(executor->GetTraceIdx(), executor->NewSpanId())
            , Executor(executor)
            , MaxTrackLength(Executor->GetAction().GetMaxTrackLength() ? Executor->GetAction().GetMaxTrackLength() : 100)
        {
        }

        bool DoAddProbe(TProbe* probe, const TParams& params, ui64 timestamp) override;
        void DoEndOfTrack() override;
        void DoDrop() override;
        void DoSerialize(TShuttleTrace& msg) override;
        bool DoFork(TShuttlePtr& child) override;
        bool DoJoin(const TShuttlePtr& child) override;

        void SetIgnore(bool ignore);
        void Clear();

        const TTrackLog& GetTrackLog() const {
            return TrackLog;
        }
    };

    ////////////////////////////////////////////////////////////////////////////////

    template <class TDepot>
    class TLogShuttleActionBase: public IExecutor {
    private:
        const ui64 TraceIdx;

    public:
        explicit TLogShuttleActionBase(ui64 traceIdx)
            : TraceIdx(traceIdx)
        {
        }
        ui64 GetTraceIdx() const {
            return TraceIdx;
        }

        static TLogShuttle<TDepot>* Cast(const TShuttlePtr& shuttle);
        static TLogShuttle<TDepot>* Cast(IShuttle* shuttle);
    };

    ////////////////////////////////////////////////////////////////////////////////

    template <class TDepot>
    class TRunLogShuttleActionExecutor: public TLogShuttleActionBase<TDepot> {
    private:
        TSpinLock Lock;
        TVector<TShuttlePtr> AllShuttles;
        TVector<TShuttlePtr> Parking;
        TRunLogShuttleAction Action;
        TDepot* Depot;

        TAtomic MissedTracks = 0;
        TAtomic* LastTrackId;
        TAtomic* LastSpanId;

        static constexpr int MaxShuttles = 100000;

    public:
        TRunLogShuttleActionExecutor(ui64 traceIdx, const TRunLogShuttleAction& action, TDepot* depot, TAtomic* lastTrackId, TAtomic* lastSpanId);
        ~TRunLogShuttleActionExecutor();
        bool DoExecute(TOrbit& orbit, const TParams& params) override;
        void RecordShuttle(TLogShuttle<TDepot>* shuttle);
        void ParkShuttle(TLogShuttle<TDepot>* shuttle);
        void DiscardShuttle();
        TShuttlePtr RentShuttle();
        ui64 NewSpanId();
        const TRunLogShuttleAction& GetAction() const {
            return Action;
        }
    };

    ////////////////////////////////////////////////////////////////////////////////

    template <class TDepot>
    class TEditLogShuttleActionExecutor: public TLogShuttleActionBase<TDepot> {
    private:
        TEditLogShuttleAction Action;

    public:
        TEditLogShuttleActionExecutor(ui64 traceIdx, const TEditLogShuttleAction& action);
        bool DoExecute(TOrbit& orbit, const TParams& params) override;
    };

    ////////////////////////////////////////////////////////////////////////////////

    template <class TDepot>
    class TDropLogShuttleActionExecutor: public TLogShuttleActionBase<TDepot> {
    private:
        TDropLogShuttleAction Action;

    public:
        TDropLogShuttleActionExecutor(ui64 traceIdx, const TDropLogShuttleAction& action);
        bool DoExecute(TOrbit& orbit, const TParams& params) override;
    };

    ////////////////////////////////////////////////////////////////////////////////

    template <class TDepot>
    bool TLogShuttle<TDepot>::DoAddProbe(TProbe* probe, const TParams& params, ui64 timestamp) {
        with_lock (Lock) {
            if (TrackLog.Items.size() >= MaxTrackLength) {
                TrackLog.Truncated = true;
                return true;
            }
            TrackLog.Items.emplace_back();
            TTrackLog::TItem* item = &TrackLog.Items.back();
            item->ThreadId = 0; // TODO[serxa]: check if it is fast to run TThread::CurrentThreadId();
            item->Probe = probe;
            if ((item->SavedParamsCount = probe->Event.Signature.ParamCount) > 0) {
                probe->Event.Signature.CloneParams(item->Params, params);
            }
            item->TimestampCycles = timestamp ? timestamp : GetCycleCount();
        }

        return true;
    }

    template <class TDepot>
    void TLogShuttle<TDepot>::DoEndOfTrack() {
        // Record track log if not ignored
        if (!Ignore) {
            if (AtomicGet(ForkFailed)) {
                Executor->DiscardShuttle();
            } else {
                Executor->RecordShuttle(this);
            }
        }
        Executor->ParkShuttle(this);
    }

    template <class TDepot>
    void TLogShuttle<TDepot>::DoDrop() {
        // Do not track log results of dropped shuttles
        Executor->ParkShuttle(this);
    }

    template <class TDepot>
    void TLogShuttle<TDepot>::SetIgnore(bool ignore) {
        Ignore = ignore;
    }

    template <class TDepot>
    void TLogShuttle<TDepot>::Clear() {
        TrackLog.Clear();
        AtomicSet(ForkFailed, 0);
    }

    template <class TDepot>
    void TLogShuttle<TDepot>::DoSerialize(TShuttleTrace& msg)
    {
        with_lock (Lock)
        {
            if (!GetTrackLog().Items.size()) {
                return ;
            }
            for (auto& record : GetTrackLog().Items) {
                auto *rec = msg.AddEvents();
                rec->SetName(record.Probe->Event.Name);
                rec->SetProvider(record.Probe->Event.GetProvider());
                rec->SetTimestampNanosec(
                    CyclesToEpochNanoseconds(record.TimestampCycles));
                record.Probe->Event.Signature.SerializeToPb(record.Params, *rec->MutableParams());
            }
        }
    }

    template <class TDepot>
    TLogShuttle<TDepot>* TLogShuttleActionBase<TDepot>::Cast(const TShuttlePtr& shuttle) {
        return static_cast<TLogShuttle<TDepot>*>(shuttle.Get());
    }

    template <class TDepot>
    TLogShuttle<TDepot>* TLogShuttleActionBase<TDepot>::Cast(IShuttle* shuttle) {
        return static_cast<TLogShuttle<TDepot>*>(shuttle);
    }

    ////////////////////////////////////////////////////////////////////////////////

    template <class TDepot>
    TRunLogShuttleActionExecutor<TDepot>::TRunLogShuttleActionExecutor(
        ui64 traceIdx,
        const TRunLogShuttleAction& action,
        TDepot* depot,
        TAtomic* lastTrackId,
        TAtomic* lastSpanId)
        : TLogShuttleActionBase<TDepot>(traceIdx)
        , Action(action)
        , Depot(depot)
        , LastTrackId(lastTrackId)
        , LastSpanId(lastSpanId)
    {
        ui64 size = Min<ui64>(Action.GetShuttlesCount() ? Action.GetShuttlesCount() : 1000, MaxShuttles); // Do not allow to allocate too much memory
        AllShuttles.reserve(size);
        Parking.reserve(size);
        for (ui64 i = 0; i < size; i++) {
            TShuttlePtr shuttle(new TLogShuttle<TDepot>(this));
            AllShuttles.emplace_back(shuttle);
            Parking.emplace_back(shuttle);
        }
    }

    template <class TDepot>
    TRunLogShuttleActionExecutor<TDepot>::~TRunLogShuttleActionExecutor() {
        for (TShuttlePtr& shuttle : AllShuttles) {
            shuttle->Kill();
        }
    }

    template <class TDepot>
    bool TRunLogShuttleActionExecutor<TDepot>::DoExecute(TOrbit& orbit, const TParams& params) {
        Y_UNUSED(params);
        if (TShuttlePtr shuttle = RentShuttle()) {
            this->Cast(shuttle)->SetIgnore(Action.GetIgnore());
            orbit.AddShuttle(shuttle);
        } else {
            AtomicIncrement(MissedTracks);
        }
        return true;
    }

    template <class TDepot>
    void TRunLogShuttleActionExecutor<TDepot>::DiscardShuttle() {
        AtomicIncrement(MissedTracks);
    }

    template <class TDepot>
    void TRunLogShuttleActionExecutor<TDepot>::RecordShuttle(TLogShuttle<TDepot>* shuttle) {
        if (Depot == nullptr) {
            return;
        }
        typename TDepot::TAccessor a(*Depot);
        if (TTrackLog* trackLog = a.Add()) {
            *trackLog = shuttle->GetTrackLog();
            trackLog->Id = AtomicIncrement(*LastTrackId); // Track id is assigned at reporting time
        }
    }

    template <class TDepot>
    TShuttlePtr TRunLogShuttleActionExecutor<TDepot>::RentShuttle() {
        TGuard<TSpinLock> g(Lock);
        if (Parking.empty()) {
            return TShuttlePtr();
        } else {
            TShuttlePtr shuttle = Parking.back();
            Parking.pop_back();
            return shuttle;
        }
    }

    template <class TDepot>
    void TRunLogShuttleActionExecutor<TDepot>::ParkShuttle(TLogShuttle<TDepot>* shuttle) {
        shuttle->Clear();
        TGuard<TSpinLock> g(Lock);
        Parking.emplace_back(shuttle);
    }

    template <class TDepot>
    ui64 TRunLogShuttleActionExecutor<TDepot>::NewSpanId()
    {
        return LastSpanId ? AtomicIncrement(*LastSpanId) : 0;
    }

    ////////////////////////////////////////////////////////////////////////////////

    template <class TDepot>
    TEditLogShuttleActionExecutor<TDepot>::TEditLogShuttleActionExecutor(ui64 traceIdx, const TEditLogShuttleAction& action)
        : TLogShuttleActionBase<TDepot>(traceIdx)
        , Action(action)
    {
    }

    template <class TDepot>
    bool TEditLogShuttleActionExecutor<TDepot>::DoExecute(TOrbit& orbit, const TParams& params) {
        Y_UNUSED(params);
        bool ignore = Action.GetIgnore();
        orbit.ForEachShuttle(this->GetTraceIdx(), [=](IShuttle* shuttle) {
            this->Cast(shuttle)->SetIgnore(ignore);
            return true;
        });
        return true;
    }

    ////////////////////////////////////////////////////////////////////////////////

    template <class TDepot>
    TDropLogShuttleActionExecutor<TDepot>::TDropLogShuttleActionExecutor(ui64 traceIdx, const TDropLogShuttleAction& action)
        : TLogShuttleActionBase<TDepot>(traceIdx)
        , Action(action)
    {
    }

    template <class TDepot>
    bool TDropLogShuttleActionExecutor<TDepot>::DoExecute(TOrbit& orbit, const TParams& params) {
        Y_UNUSED(params);
        orbit.ForEachShuttle(this->GetTraceIdx(), [](IShuttle*) {
            return false; // Erase shuttle from orbit
        });
        return true;
    }

}
