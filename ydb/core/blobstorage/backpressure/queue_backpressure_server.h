#pragma once
#include "defs.h"
#include "queue_backpressure_common.h"
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>
#include <ydb/core/base/blobstorage.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NKikimr {

    namespace NBackpressure {

        ////////////////////////////////////////////////////////////////////////////
        // TFeedback - after any action on QueueBackpressure we get this feedback
        // It contains to references that are valid until next call:
        // first -- result of the current operation (TWindowStatus)
        // second -- updates to all windows as result of global recalculation (TWindowStatusVec)
        ////////////////////////////////////////////////////////////////////////////
        template <class TClientId>
        struct TFeedback : public std::pair<const TWindowStatus<TClientId>&, const TVector<TWindowStatus<TClientId>> &> {
            using TBase = std::pair<const TWindowStatus<TClientId>&, const TVector<TWindowStatus<TClientId>> &>;

            TFeedback(const TWindowStatus<TClientId> &op, const TVector<TWindowStatus<TClientId>> &other)
                : TBase(op, other)
            {}

            bool Good() const {
                return ::NKikimr::NBackpressure::Good(TBase::first.Status);
            }

            void Output(IOutputStream &str) const {
                str << TBase::first.ToString();
            }
        };


        ////////////////////////////////////////////////////////////////////////////
        // TQueueBackpressure
        ////////////////////////////////////////////////////////////////////////////
        template <class TClientId>
        class TQueueBackpressure {
        public:
            typedef NBackpressure::TWindowStatus<TClientId> TWindowStatus;
            typedef NBackpressure::TFeedback<TClientId> TFeedback;

            ////////////////////////////////////////////////////////////////////////////
            // Stat counters for window/total
            ////////////////////////////////////////////////////////////////////////////
            struct TStat : public TThrRefBase {
                ui64 NSuccess;
                ui64 NWindowUpdate;
                ui64 NProcessed;
                ui64 NIncorrectMsgId;
                ui64 NHighWatermarkOverflow;

                void IncSuccess() { ++NSuccess; }
                void IncWindowUpdate() { ++NWindowUpdate; }
                void IncProcessed() { ++NProcessed; }
                void IncIncorrectMsgId() { ++NIncorrectMsgId; }
                void IncHighWatermarkOverflow() { ++NHighWatermarkOverflow; }

                TStat()
                    : NSuccess(0)
                    , NWindowUpdate(0)
                    , NProcessed(0)
                    , NIncorrectMsgId(0)
                    , NHighWatermarkOverflow(0)
                {}

                void Output(IOutputStream &str) const {
                    str << "NSuccess# " << NSuccess << " NWindowUpdate# " << NWindowUpdate
                    << " NProcessed# " << NProcessed << " NIncorrectMsgId# " << NIncorrectMsgId
                    << " NHighWatermarkOverflow# " << NHighWatermarkOverflow;
                }
            };
            using TStatPtr = TIntrusivePtr<TStat>;


            ////////////////////////////////////////////////////////////////////////////
            // TWindow state
            ////////////////////////////////////////////////////////////////////////////
            enum class EWindowState {
                Unknown = 0,
                Active,     // Window is in active state
                Fading,     // Window is short after active state
                Frozen,     // No activity for this window for significant time
                Dead,       // Window is subject for deletion
                Max
            };

        private:

            ////////////////////////////////////////////////////////////////////////////
            // TWindow
            ////////////////////////////////////////////////////////////////////////////
            class TWindow : public TThrRefBase {
                TStatPtr GlobalStatPtr;
            public:
                const TClientId ClientId;
                const TActorId ActorId;
            private:
                TMessageId ExpectedMsgId;
                ui64 Cost;
                ui64 LowWatermark;
                ui64 HighWatermark;
                ui64 LastMaxWindowSize;
                ui64 CostChangeUntilFrozenCountdown;
                ui64 CostChangeUntilDeathCountdown;
                const ui64 PercentThreshold;
                const ui64 DefLowWatermark;
                const ui64 DefHighWatermark;
                const ui64 CostChangeUntilFrozen;  // how much cost change we should skip to move from Fading to Frozen
                const ui64 CostChangeUntilDeath;   // how much cost change we should skip to remove window completely
                TInstant LastActivityTime;
                TDuration WindowTimeout;
                ui64 InFlight;

                void SetStatus(TWindowStatus *ptr, EStatus status, bool notify, const TMessageId &failedMsgId = {}) {
                    const ui64 maxWindowSize = Min(LowWatermark, HighWatermark);
                    if (notify) {
                        LastMaxWindowSize = maxWindowSize;
                    }
                    ptr->Set(ClientId, ActorId, status, notify, Cost, maxWindowSize, ExpectedMsgId, failedMsgId);
                }

                void UpdateCountdown(ui64 costChange) {
                    Y_DEBUG_ABORT_UNLESS(costChange);
                    if (CostChangeUntilFrozenCountdown > 0) {
                        if (CostChangeUntilFrozenCountdown >= costChange) {
                            CostChangeUntilFrozenCountdown -= costChange;
                            costChange = 0;
                        } else {
                            costChange -= CostChangeUntilFrozenCountdown;
                            CostChangeUntilFrozenCountdown = 0;
                        }
                    }

                    if (costChange) {
                        if (CostChangeUntilDeathCountdown >= costChange)
                            CostChangeUntilDeathCountdown -= costChange;
                        else
                            CostChangeUntilDeathCountdown = 0;
                    }
                }

            public:
                TWindow(TStatPtr globalStatPtr, const TClientId &clientId, const TActorId& actorId,
                        ui64 percentThreshold, ui64 defLowWatermark, ui64 defHighWatermark, ui64 costChangeUntilFrozen,
                        ui64 costChangeUntilDeath, TDuration windowTimeout, TInstant now)
                    : GlobalStatPtr(globalStatPtr)
                    , ClientId(clientId)
                    , ActorId(actorId)
                    , ExpectedMsgId()
                    , Cost(0)
                    , LowWatermark(defLowWatermark)
                    , HighWatermark(defHighWatermark)
                    , LastMaxWindowSize(0)
                    , CostChangeUntilFrozenCountdown(costChangeUntilFrozen)
                    , CostChangeUntilDeathCountdown(costChangeUntilDeath)
                    , PercentThreshold(percentThreshold)
                    , DefLowWatermark(defLowWatermark)
                    , DefHighWatermark(defHighWatermark)
                    , CostChangeUntilFrozen(costChangeUntilFrozen)
                    , CostChangeUntilDeath(costChangeUntilDeath)
                    , LastActivityTime(now)
                    , WindowTimeout(windowTimeout)
                    , InFlight(0)
                {}

                void Push(bool checkMsgId, const TMessageId &msgId, ui64 cost, TWindowStatus *opStatus,
                        TInstant now) {
                    Y_DEBUG_ABORT_UNLESS(LowWatermark != 0);
                    Y_DEBUG_ABORT_UNLESS(LowWatermark <= HighWatermark);
                    Y_DEBUG_ABORT_UNLESS(cost > 0);

                    if ((msgId.MsgId == ExpectedMsgId.MsgId && msgId.SequenceId >= ExpectedMsgId.SequenceId) || !checkMsgId) {
                        // accept if msgId is correct
                        if (Cost + cost <= HighWatermark || InFlight == 0) { // accept at least one message of any cost
                            GlobalStatPtr->IncSuccess();
                            CostChangeUntilFrozenCountdown = CostChangeUntilFrozen;
                            CostChangeUntilDeathCountdown = CostChangeUntilDeath;
                            LastActivityTime = now;
                            Cost += cost;
                            ++InFlight;
                            ExpectedMsgId = TMessageId(msgId.SequenceId, msgId.MsgId + 1);
                            SetStatus(opStatus, NKikimrBlobStorage::TWindowFeedback::Success, false);
                        } else {
                            // reject message
                            ExpectedMsgId.SequenceId = Max(ExpectedMsgId.SequenceId, msgId.SequenceId + 1);
                            SetStatus(opStatus, NKikimrBlobStorage::TWindowFeedback::HighWatermarkOverflow, true, msgId);
                            GlobalStatPtr->IncHighWatermarkOverflow();
                        }
                    } else {
                        ExpectedMsgId.SequenceId = Max(ExpectedMsgId.SequenceId, msgId.SequenceId + 1);
                        SetStatus(opStatus, NKikimrBlobStorage::TWindowFeedback::IncorrectMsgId, true, msgId);
                        GlobalStatPtr->IncIncorrectMsgId();
                    }
                }

                TWindowStatus *Processed(bool /*checkMsgId*/, const TMessageId& /*msgId*/, ui64 cost, TWindowStatus *opStatus) {
                    Y_ABORT_UNLESS(Cost >= cost);
                    Cost -= cost;
                    --InFlight;
                    SetStatus(opStatus, NKikimrBlobStorage::TWindowFeedback::Processed, true);
                    GlobalStatPtr->IncProcessed();
                    return opStatus;
                }

                TWindowStatus *AdjustWindow(ui64 lowWatermark, ui64 highWatermark, TWindowStatus *opStatus) {
                    // definitely notify client if it goes to receive rejects
                    bool notify = highWatermark < LowWatermark;

                    LowWatermark = lowWatermark;
                    HighWatermark = highWatermark;

                    if (!notify && LastMaxWindowSize != 0) {
                        // i.e. do not notify and we have last set window sets up =>
                        // ... check difference between LastMaxWindowSize and LowWatermark
                        ui64 diff = (LowWatermark >= LastMaxWindowSize) ?
                            (LowWatermark - LastMaxWindowSize) : (LastMaxWindowSize - LowWatermark);
                        Y_ABORT_UNLESS(LowWatermark != 0);
                        notify = (diff * 100u / LowWatermark >= PercentThreshold);
                    }

                    SetStatus(opStatus, NKikimrBlobStorage::TWindowFeedback::WindowUpdate, notify);
                    GlobalStatPtr->IncWindowUpdate();
                    return opStatus;
                }

                EWindowState GetState(TInstant now) const {
                    if (Cost > 0 || InFlight) {
                        return EWindowState::Active;
                    } else if (LowWatermark > DefLowWatermark || CostChangeUntilFrozenCountdown > 0) {
                        return EWindowState::Fading;
                    } else if (CostChangeUntilDeathCountdown > 0 || now < LastActivityTime + WindowTimeout) {
                        return EWindowState::Frozen;
                    } else {
                        Y_ASSERT(CostChangeUntilFrozenCountdown == 0 && CostChangeUntilDeathCountdown == 0 &&
                            now >= LastActivityTime + WindowTimeout);
                        return EWindowState::Dead;
                    }
                }

                void Fade(EWindowState state, ui64 costChange, TInstant now) {
                    Y_ABORT_UNLESS(state == GetState(now));

                    // TODO: we can implement more gracefull fading, take into account that beside this fading
                    //       we recalculate windows on regular basis
                    switch (state) {
                        case EWindowState::Active:
                        case EWindowState::Fading:
                        case EWindowState::Frozen:
                            break;
                        default: Y_ABORT("Unexpected case");
                    }
                    UpdateCountdown(costChange);
                }

                ui64 GetCost() const {
                    return Cost;
                }

                TMessageId GetExpectedMsgId() const {
                    return ExpectedMsgId;
                }

                void Output(IOutputStream &str) const {
                    str << "ClientId# " << ClientId << " ExpectedMsgId# " << ExpectedMsgId
                    << " Cost# " << Cost << " LowWatermark# " << LowWatermark
                    << " HighWatermark# " << HighWatermark
                    << " CostChangeUntilFrozenCountdown# " << CostChangeUntilFrozenCountdown
                    << " CostChangeUntilDeathCountdown# " << CostChangeUntilDeathCountdown;
                }
            };

            typedef TIntrusivePtr<TWindow> TWindowPtr;
            typedef THashMap<TActorId, TWindowPtr> TAllWindows;
            typedef TVector<TWindowPtr> TWindowPtrVec;

            const bool CheckMsgId;
            const ui64 MaxCost;
            const ui64 CostChangeToRecalculate;
            const ui64 MinLowWatermark;
            const ui64 MaxLowWatermark;
            const ui64 PercentThreshold;
            const ui64 CostChangeUntilFrozen;
            const ui64 CostChangeUntilDeath;
            ui64 CostChange;
            TAllWindows AllWindows;
            TWindowPtrVec ActiveWindowsCache;
            TWindowStatus StatusCache;
            TWindowStatus RecalculateStatusCache;
            TVector<TWindowStatus> OtherWindowsStatusVecCache;
            TStatPtr GlobalStatPtr;
            TDuration WindowTimeout;


            void ClearRecalculateCache() {
                // OtherWindowsStatusVecCache has to be clear on every call to Push/Processed,
                // it filled in only sometimes, when CostChange is enough to make recalculation
                OtherWindowsStatusVecCache.clear();
            }

            void Recalculate(const TActorId& actorId, bool force, TInstant now) {
                if (CostChange < CostChangeToRecalculate && !force)
                    return;

                ui64 actualCostChange = CostChange;
                CostChange = 0;
                ActiveWindowsCache.clear();
                OtherWindowsStatusVecCache.clear();
                ui64 totalCost = 0;

                // gather active windows stat
                using TIterator = typename TAllWindows::iterator;
                TIterator it = AllWindows.begin();
                while (it != AllWindows.end()) {
                    TWindowPtr wPtr = it->second;
                    EWindowState windowState = wPtr->GetState(now);
                    switch (windowState) {
                        case EWindowState::Active: {
                            ActiveWindowsCache.push_back(wPtr);
                            totalCost += wPtr->GetCost();
                            wPtr->Fade(windowState, actualCostChange, now);
                            ++it;
                            break;
                        }
                        case EWindowState::Fading: {
                            // until frozen we use this window for recalculation
                            Y_ABORT_UNLESS(wPtr->GetCost() == 0);
                            ActiveWindowsCache.push_back(wPtr);
                            totalCost += wPtr->GetCost();
                            wPtr->Fade(windowState, actualCostChange, now);
                            ++it;
                            break;
                        }
                        case EWindowState::Frozen: {
                            Y_ABORT_UNLESS(wPtr->GetCost() == 0);
                            wPtr->Fade(windowState, actualCostChange, now);
                            ++it;
                            break;
                        }
                        case EWindowState::Dead: {
                            Y_ABORT_UNLESS(wPtr->GetCost() == 0);
                            TIterator prev = it++;
                            AllWindows.erase(prev);
                            break;
                        }
                        default:
                            Y_ABORT("Unexpected case");
                    }
                }

                // recalculate boundaries for active windows
                size_t activeWindowsNum = ActiveWindowsCache.size();
                if (activeWindowsNum) {
                    if (totalCost == 0) {
                        // if all windows are in fading mode totalCost can be 0
                        totalCost = MaxCost;
                    }
                    // calculate low watermark
                    ui64 lowWatermark = MaxCost / activeWindowsNum;
                    lowWatermark = Max(lowWatermark, MinLowWatermark);
                    lowWatermark = Min(lowWatermark, MaxLowWatermark);

                    // adjust active windows
                    for (auto &a : ActiveWindowsCache) {
                        // calculate high watermark as weighted fraction of MaxCost
                        ui64 highWatermark = ui64(MaxCost) * ui64(a->GetCost()) / totalCost;
                        highWatermark = Max(highWatermark, lowWatermark); // highWatermark can be 0 for fading windows
                        // adjust window
                        TWindowStatus *update = a->AdjustWindow(lowWatermark, highWatermark, &RecalculateStatusCache);
                        if (update->Notify) {
                            if (a->ActorId == actorId) {
                                StatusCache.WindowUpdate(*update);
                            } else {
                                OtherWindowsStatusVecCache.emplace_back(*update);
                            }
                        }
                    }
                }
            }

        public:
            TQueueBackpressure(bool checkMsgId, ui64 maxCost = 100u, ui64 costChangeToRecalculate = 10u,
                               ui64 minLowWatermark = 2u, ui64 maxLowWatermark = 20u, ui64 percentThreshold = 10u,
                               ui64 costChangeUntilFrozen = 20u, ui64 costChangeUntilDeath = 30u,
                               TDuration windowTimeout = TDuration::Seconds(20))
                : CheckMsgId(checkMsgId)
                , MaxCost(maxCost)
                , CostChangeToRecalculate(costChangeToRecalculate)
                , MinLowWatermark(minLowWatermark)
                , MaxLowWatermark(maxLowWatermark)
                , PercentThreshold(percentThreshold)
                , CostChangeUntilFrozen(costChangeUntilFrozen)
                , CostChangeUntilDeath(costChangeUntilDeath)
                , CostChange(0)
                , AllWindows()
                , ActiveWindowsCache()
                , StatusCache()
                , RecalculateStatusCache()
                , OtherWindowsStatusVecCache()
                , GlobalStatPtr(new TStat())
                , WindowTimeout(windowTimeout)
            {}

            std::optional<TMessageId> GetExpectedMsgId(const TActorId& actorId) const {
                if (const auto it = AllWindows.find(actorId); it != AllWindows.end()) {
                    return it->second->GetExpectedMsgId();
                } else {
                    return std::nullopt;
                }
            }

            TFeedback Push(const TClientId& clientId, const TActorId& actorId, const TMessageId &msgId, ui64 cost, TInstant now) {
                Y_ABORT_UNLESS(actorId);
                ClearRecalculateCache();

                // find or create window
                bool newWindow = false;
                auto it = AllWindows.find(actorId);
                if (it == AllWindows.end()) {
                    // first connect, create window
                    ui64 defWindowLowWatermark = MinLowWatermark;
                    ui64 defWindowHighWatermark = MaxLowWatermark;
                    TWindowPtr window(new TWindow(GlobalStatPtr, clientId, actorId, PercentThreshold,
                                                  defWindowLowWatermark, defWindowHighWatermark,
                                                  CostChangeUntilFrozen, CostChangeUntilDeath,
                                                  WindowTimeout, now));
                    auto res = AllWindows.emplace(actorId, window);
                    it = res.first;
                    Y_ABORT_UNLESS(res.second);
                    newWindow = true;
                }
                TWindow &window = *(it->second);

                // perform action
                window.Push(CheckMsgId, msgId, cost, &StatusCache, now);
                CostChange += Good(StatusCache.Status) ? cost : 0;
                Recalculate(actorId, newWindow && Good(StatusCache.Status), now);
                return TFeedback(StatusCache, OtherWindowsStatusVecCache);
            }

            TFeedback Processed(const TActorId& actorId, const TMessageId &msgId, ui64 cost, TInstant now) {
                ClearRecalculateCache();

                // find window
                auto it = AllWindows.find(actorId);
                Y_ABORT_UNLESS(it != AllWindows.end());

                TWindow &window = *(it->second);
                // perform action
                window.Processed(CheckMsgId, msgId, cost, &StatusCache);
                CostChange += cost;
                Recalculate(actorId, false, now);
                return TFeedback(StatusCache, OtherWindowsStatusVecCache);
            }

            template<typename TCallback>
            void ForEachWindow(TCallback&& callback) {
                for (const auto& [actorId, window] : AllWindows) {
                    callback(window);
                }
            }

            void Output(IOutputStream &str, TInstant now) const {
                str << "MaxCost# " << MaxCost;
                ui64 actualCost = 0;
                ui32 windowTypesHisto[unsigned(EWindowState::Max)];
                ForEach(windowTypesHisto, windowTypesHisto + unsigned(EWindowState::Max), [](ui32 &x) {x = 0;});
                for (const auto &x : AllWindows) {
                    actualCost += x.second->GetCost();
                    windowTypesHisto[unsigned(x.second->GetState(now))]++;
                }
                str << " ActualCost# " << actualCost
                << " activeWindows# " << windowTypesHisto[unsigned(EWindowState::Active)]
                << " fadingWindows# " << windowTypesHisto[unsigned(EWindowState::Fading)]
                << " frozenWindows# " << windowTypesHisto[unsigned(EWindowState::Frozen)]
                << " deadWindows# " << windowTypesHisto[unsigned(EWindowState::Dead)];
                str << "\n";
                str << "GlobalStat: ";
                GlobalStatPtr->Output(str);
                str << "\n";
                for (const auto &x : AllWindows) {
                    x.second->Output(str);
                    str << "\n";
                }
            }
        };

    } // NBackpressure

} // NKikimr

template<>
inline void Out<NKikimr::NBackpressure::TMessageId>(IOutputStream& o,
                                                    const NKikimr::NBackpressure::TMessageId &x) {
    return x.Out(o);
}

template<>
inline void Out<NKikimr::NBackpressure::TQueueClientId>(IOutputStream& o,
                                                        const NKikimr::NBackpressure::TQueueClientId &x) {
    return x.Output(o);
}

template<>
struct THash<NKikimr::NBackpressure::TQueueClientId> {
    size_t operator ()(const NKikimr::NBackpressure::TQueueClientId& clientId) const {
        return clientId.Hash();
    }
};
