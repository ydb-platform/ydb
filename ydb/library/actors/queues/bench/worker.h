#pragma once

#include "defs.h"

#include <memory>
#include <util/random/random.h>
#include <util/system/thread.h>


namespace NActors::NQueueBench { 

    enum class EThreadAction {
        Continue,
        Sleep,
        Kill,
    };

    struct TThreadAction {
        EThreadAction Action;
        ui64 SleepNs = 0;
    };

    enum class EWorkerAction {
        Push,
        Pop,
        Sleep,
        Kill,
    };

    enum class EExpectedStatus {
        Nothing,
        Success,
        Failure,
        RepeatUntilSuccess,
    };

    struct TWorkerAction {
        EWorkerAction Action;
        EExpectedStatus Expected = EExpectedStatus::Nothing;
        std::optional<ui64> Value;

        explicit operator TThreadAction() const {
            switch (Action) {
            case EWorkerAction::Push:
            case EWorkerAction::Pop:
                return {.Action=EThreadAction::Continue};
            case EWorkerAction::Sleep:
                Y_ABORT_UNLESS(Value);
                return {.Action=EThreadAction::Sleep, .SleepNs=*Value};
            case EWorkerAction::Kill:
                return {.Action=EThreadAction::Kill};
            }
        }
    };

    template <typename TQueue>
    class TSimpleWorker {
    public:
        TSimpleWorker(TQueue *queue, const std::vector<TWorkerAction> actions, ui32 repeatCount = 1)
            : Queue(queue)
            , Actions(actions)
            , RepeatCount(repeatCount)
        {
            Y_ABORT_UNLESS(actions.size());
        }

        TThreadAction Do() {
            if (Idx == Actions.size()) {
                Idx = 0;
                if (--RepeatCount == 0) {
                    return {.Action=EThreadAction::Kill};
                }
            }
            TWorkerAction &action = Actions[Idx++];
            switch (action.Action) {
            case EWorkerAction::Push: {
                    Y_ABORT_UNLESS(action.Value);
                    bool success = Queue->TryPush(*action.Value);
                    if (action.Expected == EExpectedStatus::RepeatUntilSuccess) {
                        while (!success) {
                            success = Queue->TryPush(*action.Value);
                        }
                    } else if (action.Expected != EExpectedStatus::Nothing) {
                        Y_ABORT_UNLESS(success == (action.Expected == EExpectedStatus::Success));
                    }
                }
                break;
            case EWorkerAction::Pop: {
                    auto value = Queue->TryPop();
                    if (action.Expected == EExpectedStatus::RepeatUntilSuccess) {
                        while (!value) {
                            value = Queue->TryPop();
                        }
                    } else if (action.Expected != EExpectedStatus::Nothing) {
                        Y_ABORT_UNLESS(bool(value) == (action.Expected == EExpectedStatus::Success));
                        if (value && action.Value) {
                            Y_ABORT_UNLESS(value == action.Value);
                        }
                    }
                }
                break;
            default:
                break;
            }
            return static_cast<TThreadAction>(action);
        }

    private:
        TQueue *Queue;
        std::vector<TWorkerAction> Actions;
        ui32 Idx = 0;
        ui32 RepeatCount = 0;
    };

    template <typename TQueue>
    class TProducerWorker {
    public:
        TProducerWorker(TQueue *queue, std::optional<ui32> writes)
            : Queue(queue)
            , Writes(writes)
        {}

        TThreadAction Do() {
            if (Writes) {
                if (Written == *Writes) {
                    return {.Action=EThreadAction::Kill};
                }
                auto value = Queue->TryPush(Written);
                if (value) {
                    Written++;
                }
            } else {
                auto success = Queue->TryPush(Written);
                if (!success) {
                    return {.Action=EThreadAction::Kill};
                }
                Written++;
            }
            return {.Action=EThreadAction::Continue};
        }

        TQueue *Queue;
        std::optional<ui32> Writes;
        ui64 Written = 0;
    };

    struct TConsumerInfo {
        std::vector<std::optional<ui32>> ReadedItems;
    };

    template <typename TQueue>
    class TConsumerWorker {
    public:
        TConsumerWorker(TQueue *queue, std::optional<ui32> reads, bool countFailsAsReads=false, TConsumerInfo *info=nullptr)
            : Queue(queue)
            , Reads(reads)
            , CountFailsAsReads(countFailsAsReads)
            , Info(info)
        {}

        TThreadAction Do() {
            if (Reads) {
                if (Readed == *Reads) {
                    return {.Action=EThreadAction::Kill};
                }
                auto value = Queue->TryPop();
                if (value || CountFailsAsReads) {
                    Readed++;
                    if (Info) {
                        Info->ReadedItems.emplace_back(std::move(value));
                    }
                }
            } else {
                auto value = Queue->TryPop();
                if (!value) {
                    return {.Action=EThreadAction::Kill};
                }
                Readed++;
                if (Info) {
                    Info->ReadedItems.emplace_back(std::move(value));
                }
            }
            return {.Action=EThreadAction::Continue};
        }

        TQueue *Queue;
        std::optional<ui32> Reads;
        ui64 Readed = 0;
        bool CountFailsAsReads;
        TConsumerInfo *Info;
    };


    template <typename TAction>
    class TWorkerWithDuration {
    public:
        TWorkerWithDuration(TAction action, TDuration duration)
            : Action(action)
            , Duration(duration)
            , StartTime()
        {}

        TThreadAction Do() {
            if (!StartTime) {
                StartTime = TInstant::Now();
            } else if (++Iteration % 1024 == 0 && TInstant::Now() - StartTime > Duration) {
                return {.Action=EThreadAction::Kill};
            }
            if (Action()) {
                return {.Action=EThreadAction::Continue};
            }
            return {.Action=EThreadAction::Kill};
        }

        TAction Action;
        TDuration Duration;
        TInstant StartTime;
        ui32 Iteration = 0;
    };

    template <typename TWorker, typename TStatsCollector=void>
    class TTestThread : public ISimpleThread {
    public:
        TTestThread(TWorker worker, TStatsCollector *statsCollector = nullptr)
            : Worker(worker)
            , StatsCollector(statsCollector)
        {}

        ~TTestThread() = default;

    private:
        bool Process(const TThreadAction &action) {
            switch (action.Action) {
            case EThreadAction::Continue:
                break;
            case EThreadAction::Sleep:
                NanoSleep(action.SleepNs);
                break;
            case EThreadAction::Kill:
                if constexpr (!std::is_same_v<std::decay_t<TStatsCollector>, void>) {
                    if (StatsCollector) {
                        auto stats = TStatsCollector::TStatsSource::GetLocalStats();
                        // Cerr << (TStringBuilder() << "thread: " << (ui64)this << " pushes: " << stats.SuccessPushes.load() << Endl);
                        StatsCollector->AddStats(stats);
                    }
                }
                return true;
            }
            return false;
        }

        void* ThreadProc() final {
            for (;;) {
                TThreadAction action = Worker.Do();
                if (Process(action)) {
                    break;
                }
            }
            return nullptr;
        }

    private:
        TWorker Worker;
        TStatsCollector *StatsCollector;
    };

    inline void RunThreads(const std::vector<ISimpleThread*> &threads) {
        for (auto &thread : threads) {
            thread->Start();
        }
        for (auto &thread : threads) {
            thread->Join();
        }
    }

    inline void RunThreads(const std::vector<std::unique_ptr<ISimpleThread>> &threads) {
        for (auto &thread : threads) {
            thread->Start();
        }
        for (auto &thread : threads) {
            thread->Join();
        }
    }

    template <typename ...TThreads>
    inline void RunThreads(std::unique_ptr<TThreads>&& ...threads) {
        RunThreads(std::vector<ISimpleThread*>{threads.release()...});
    }

}