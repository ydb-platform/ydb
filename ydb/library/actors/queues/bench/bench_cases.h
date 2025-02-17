#pragma once

#include "defs.h"
#include "queue.h"
#include "worker.h"

#include <util/string/builder.h>
#include <util/random/fast.h>


namespace NActors::NQueueBench {

    struct TThreadCounts {
        ui64 ProducerThreads = 0;
        ui64 ConsumerThreads = 0;
    };

    template <typename TStatsCollector>
    class IBenchCase {
    public:
        virtual ~IBenchCase () {}

        virtual TStatsCollector Run() = 0;

        virtual TThreadCounts GetThreads(ui64 globalThreads) = 0;
    };

    template <typename TStatsCollector>
    class IBenchCaseWithDurationAndThreads {
    public:
        virtual ~IBenchCaseWithDurationAndThreads() {}

        virtual TStatsCollector Run(TDuration duration, ui64 threads) = 0;

        virtual TString Validate(ui64 /*threads*/) {
            return "";
        }

        virtual std::variant<TThreadCounts, ui64> GetThreads(ui64 globalThreads) = 0;
    };
    
    template <typename TRealQueue, typename TQueueAdaptor>
    struct TTestCases {
        
        template <typename TStatsCollector>
        class TBasicPushPopSingleThread : public IBenchCase<TStatsCollector> {
        public:
            TStatsCollector Run() override {
                TRealQueue realQueue;
                TQueueAdaptor adapter(&realQueue);
                TStatsCollector collector;
                TSimpleWorker<decltype(adapter)> worker(
                    &adapter,
                    {
                        TWorkerAction{.Action=EWorkerAction::Push, .Expected=EExpectedStatus::Success, .Value=1},
                        TWorkerAction{.Action=EWorkerAction::Push, .Expected=EExpectedStatus::Success, .Value=2},
                        TWorkerAction{.Action=EWorkerAction::Pop, .Expected=EExpectedStatus::Success, .Value=1},
                        TWorkerAction{.Action=EWorkerAction::Pop, .Expected=EExpectedStatus::Success, .Value=2},
                    }
                );
                RunThreads(std::make_unique<TTestThread<decltype(worker), TStatsCollector>>(worker, &collector));
                return std::move(collector);
            }

            TThreadCounts GetThreads(ui64) override {
                return {1, 1};
            }
        };

        template <typename TStatsCollector, ui32 ThreadCount, ui32 RepeatCount>
        class TBasicPushPopMultiThreads : public IBenchCase<TStatsCollector> {
        public:
            TStatsCollector Run() override {
                TRealQueue realQueue;
                TVector<std::unique_ptr<IQueue>> adapters;
                TVector<std::unique_ptr<ISimpleThread>> threads;
                TStatsCollector collector;
                for (ui32 threadIdx = 0; threadIdx < ThreadCount; ++threadIdx) {
                    TQueueAdaptor *adapter = new TQueueAdaptor(&realQueue);
                    adapters.emplace_back(adapter);
                    TSimpleWorker<std::decay_t<decltype(*adapter)>> worker(
                        adapter,
                        {
                            TWorkerAction{.Action=EWorkerAction::Push, .Expected=EExpectedStatus::Success, .Value=1},
                            TWorkerAction{.Action=EWorkerAction::Pop, .Expected=EExpectedStatus::Success, .Value=1},
                        },
                        RepeatCount
                    );
                    threads.emplace_back(new TTestThread<decltype(worker), TStatsCollector>(worker, &collector));
                }
                RunThreads(threads);
                return std::move(collector);
            }

            TThreadCounts GetThreads(ui64) override {
                return {ThreadCount, ThreadCount};
            }
        };
        
        template <typename TStatsCollector, ui32 ThreadCount>
        class TBasicProducing : public IBenchCase<TStatsCollector> {
        public:
            TStatsCollector Run() override {
                TRealQueue realQueue;
                TVector<std::unique_ptr<IQueue>> adapters;
                TVector<std::unique_ptr<ISimpleThread>> threads;
                TStatsCollector collector;
                for (ui32 threadIdx = 0; threadIdx < ThreadCount; ++threadIdx) {
                    TQueueAdaptor *adapter = new TQueueAdaptor(&realQueue);
                    adapters.emplace_back(adapter);
                    TProducerWorker<std::decay_t<decltype(*adapter)>> worker(adapter, {});
                    threads.emplace_back(new TTestThread<decltype(worker), TStatsCollector>(worker, &collector));
                }
                RunThreads(threads);
                return std::move(collector);
            }

            TThreadCounts GetThreads(ui64) override {
                return {ThreadCount, 0};
            }
        };
        
        template <typename TStatsCollector, ui32 ThreadCount, ui32 Reads>
        class TConsumingEmptyQueue : public IBenchCase<TStatsCollector> {
        public:
            TStatsCollector Run() override {
                TRealQueue realQueue;
                TVector<std::unique_ptr<IQueue>> adapters;
                TVector<std::unique_ptr<ISimpleThread>> threads;
                TStatsCollector collector;
                for (ui32 threadIdx = 0; threadIdx < ThreadCount; ++threadIdx) {
                    TQueueAdaptor *adapter = new TQueueAdaptor(&realQueue);
                    adapters.emplace_back(adapter);
                    TConsumerWorker<std::decay_t<decltype(*adapter)>> worker(adapter, Reads, true);
                    threads.emplace_back(new TTestThread<decltype(worker), TStatsCollector>(worker, &collector));
                }
                RunThreads(threads);
                return std::move(collector);
            }

            TThreadCounts GetThreads(ui64) override {
                return {0, ThreadCount};
            }
        };

        template <typename TStatsCollector, ui32 ProducingThreadCount, ui32 ConsumingThreadCount, ui32 PushedItems, ui32 PoppedItems>
        class TBasicProducingConsuming : public IBenchCase<TStatsCollector> {
        public:
            TStatsCollector Run() override {
                TRealQueue realQueue;
                TVector<std::unique_ptr<IQueue>> adapters;
                TVector<std::unique_ptr<ISimpleThread>> threads;
                TStatsCollector collector;
                for (ui32 threadIdx = 0; threadIdx < ProducingThreadCount; ++threadIdx) {
                    TQueueAdaptor *adapter = new TQueueAdaptor(&realQueue);
                    adapters.emplace_back(adapter);
                    TProducerWorker<std::decay_t<decltype(*adapter)>> worker(adapter, PushedItems);
                    threads.emplace_back(new TTestThread<decltype(worker), TStatsCollector>(worker, &collector));
                }
                collector.ConsumerInfo.resize(ConsumingThreadCount);
                for (ui32 threadIdx = 0; threadIdx < ConsumingThreadCount; ++threadIdx) {
                    TQueueAdaptor *adapter = new TQueueAdaptor(&realQueue);
                    adapters.emplace_back(adapter);
                    TConsumerWorker<std::decay_t<decltype(*adapter)>> worker(adapter, PoppedItems, false, &collector.ConsumerInfo[threadIdx]);
                    threads.emplace_back(new TTestThread<decltype(worker), TStatsCollector>(worker, &collector));
                }
                RunThreads(threads);
                return std::move(collector);
            }

            TThreadCounts GetThreads(ui64) override {
                return {ProducingThreadCount, ConsumingThreadCount};
            }
        };
    };

        
    template <typename TRealQueue, typename TQueueAdaptor>
    struct TBenchCasesWithDurationAndThreads {

        template <typename TStatsCollector, bool Sleep>
        class TBasicPushPop : public IBenchCaseWithDurationAndThreads<TStatsCollector> {
            TStatsCollector Run(TDuration duration, ui64 threadCount) override {
                TRealQueue realQueue;
                TVector<std::unique_ptr<IQueue>> adapters;
                TVector<std::unique_ptr<ISimpleThread>> threads;
                TStatsCollector collector;
                for (ui32 threadIdx = 0; threadIdx < threadCount; ++threadIdx) {
                    TQueueAdaptor* adapter = new TQueueAdaptor(&realQueue);
                    adapters.emplace_back(adapter);
                    auto action = [queue = adapter] () -> bool {
                        while (!queue->TryPush(727)) {
                            if constexpr (Sleep) {
                                NanoSleep(1'000);
                            } else {
                                SpinLockPause();
                            }
                        }
                        while (!queue->TryPop()) {
                            if constexpr (Sleep) {
                                NanoSleep(1'000);
                            } else {
                                SpinLockPause();
                            }
                        }
                        return true;
                    };
                    TWorkerWithDuration<std::decay_t<decltype(action)>> worker(action, duration);
                    threads.emplace_back(new TTestThread<decltype(worker), TStatsCollector>(worker, &collector));
                }
                RunThreads(threads);
                return std::move(collector);
            }

            std::variant<TThreadCounts, ui64> GetThreads(ui64 globalThreads) override {
                return globalThreads;
            }
        };

        template <typename TStatsCollector, ui64 Producers, ui64 Consumers, bool Sleep>
        class TBasicProducingConsuming : public IBenchCaseWithDurationAndThreads<TStatsCollector> {
            TStatsCollector Run(TDuration duration, ui64 threadCount) override {
                TRealQueue realQueue;
                TVector<std::unique_ptr<IQueue>> adapters;
                TVector<std::unique_ptr<ISimpleThread>> threads;
                TStatsCollector collector;
                ui64 producingThreadCount = Producers * (threadCount / (Producers + Consumers));
                ui64 consumingThreadCount = Consumers * (threadCount / (Producers + Consumers));
                for (ui32 threadIdx = 0; threadIdx < producingThreadCount; ++threadIdx) {
                    TQueueAdaptor *adapter = new TQueueAdaptor(&realQueue);
                    adapters.emplace_back(adapter);
                    auto action = [queue = adapter] () -> bool {
                        if (!queue->TryPush(727)) {
                            if constexpr (Sleep) {
                                NanoSleep(1'000);
                            } else {
                                SpinLockPause();
                            }
                        }
                        return true;
                    };
                    TWorkerWithDuration<std::decay_t<decltype(action)>> worker(action, duration);
                    threads.emplace_back(new TTestThread<decltype(worker), TStatsCollector>(worker, &collector));
                }
                for (ui32 threadIdx = 0; threadIdx < consumingThreadCount; ++threadIdx) {
                    TQueueAdaptor *adapter = new TQueueAdaptor(&realQueue);
                    adapters.emplace_back(adapter);
                    auto action = [queue = adapter] () -> bool {
                        if (!queue->TryPop()) {
                            if constexpr (Sleep) {
                                NanoSleep(1'000);
                            } else {
                                SpinLockPause();
                            }
                        }
                        return true;
                    };
                    TWorkerWithDuration<std::decay_t<decltype(action)>> worker(action, duration);
                    threads.emplace_back(new TTestThread<decltype(worker), TStatsCollector>(worker, &collector));
                }
                RunThreads(threads);
                return std::move(collector);
            }

            TString Validate(ui64 globalThreads) override {
                if (globalThreads % (Producers + Consumers)) {
                    return TStringBuilder() << "A number divisible by " << Producers + Consumers << " was expected instead of " << globalThreads;
                }
                return "";
            }

            std::variant<TThreadCounts, ui64> GetThreads(ui64 globalThreads) override {
                ui64 producingThreadCount = Producers * (globalThreads / (Producers + Consumers));
                ui64 consumingThreadCount = Consumers * (globalThreads / (Producers + Consumers));
                return TThreadCounts{producingThreadCount, consumingThreadCount};
            }
        };

        template <typename TStatsCollector, bool Sleep>
        class TSingleProducer: public IBenchCaseWithDurationAndThreads<TStatsCollector> {
            TStatsCollector Run(TDuration duration, ui64 threadCount) override {
                TRealQueue realQueue;
                TVector<std::unique_ptr<IQueue>> adapters;
                TVector<std::unique_ptr<ISimpleThread>> threads;
                TStatsCollector collector;
                ui64 producingThreadCount = 1;
                ui64 consumingThreadCount = threadCount - 1;
                for (ui32 threadIdx = 0; threadIdx < producingThreadCount; ++threadIdx) {
                    TQueueAdaptor *adapter = new TQueueAdaptor(&realQueue);
                    adapters.emplace_back(adapter);
                    auto action = [queue = adapter] () -> bool {
                        if (!queue->TryPush(727)) {
                            if constexpr (Sleep) {
                                NanoSleep(1'000);
                            } else {
                                SpinLockPause();
                            }
                        }
                        return true;
                    };
                    TWorkerWithDuration<std::decay_t<decltype(action)>> worker(action, duration);
                    threads.emplace_back(new TTestThread<decltype(worker), TStatsCollector>(worker, &collector));
                }
                for (ui32 threadIdx = 0; threadIdx < consumingThreadCount; ++threadIdx) {
                    TQueueAdaptor *adapter = new TQueueAdaptor(&realQueue);
                    adapters.emplace_back(adapter);
                    auto action = [queue = adapter] () -> bool {
                        if (!queue->TryPop()) {
                            if constexpr (Sleep) {
                                NanoSleep(1'000);
                            } else {
                                SpinLockPause();
                            }
                        }
                        return true;
                    };
                    TWorkerWithDuration<std::decay_t<decltype(action)>> worker(action, duration);
                    threads.emplace_back(new TTestThread<decltype(worker), TStatsCollector>(worker, &collector));
                }
                RunThreads(threads);
                return std::move(collector);
            }

            TString Validate(ui64 globalThreads) override {
                if (globalThreads < 2) {
                    return TStringBuilder() << "A number greater than 1 was expected instead of " << globalThreads;
                }
                return "";
            }

            std::variant<TThreadCounts, ui64> GetThreads(ui64 globalThreads) override {
                return TThreadCounts{1, globalThreads - 1};
            }
        };

        template <typename TStatsCollector, bool Sleep>
        class TSingleConsumer: public IBenchCaseWithDurationAndThreads<TStatsCollector> {
            TStatsCollector Run(TDuration duration, ui64 threadCount) override {
                TRealQueue realQueue;
                TVector<std::unique_ptr<IQueue>> adapters;
                TVector<std::unique_ptr<ISimpleThread>> threads;
                TStatsCollector collector;
                ui64 producingThreadCount = threadCount - 1;
                ui64 consumingThreadCount = 1;
                for (ui32 threadIdx = 0; threadIdx < producingThreadCount; ++threadIdx) {
                    TQueueAdaptor *adapter = new TQueueAdaptor(&realQueue);
                    adapters.emplace_back(adapter);
                    auto action = [queue = adapter] () -> bool {
                        if (!queue->TryPush(727)) {
                            if constexpr (Sleep) {
                                NanoSleep(1'000);
                            } else {
                                SpinLockPause();
                            }
                        }
                        return true;
                    };
                    TWorkerWithDuration<std::decay_t<decltype(action)>> worker(action, duration);
                    threads.emplace_back(new TTestThread<decltype(worker), TStatsCollector>(worker, &collector));
                }
                for (ui32 threadIdx = 0; threadIdx < consumingThreadCount; ++threadIdx) {
                    TQueueAdaptor *adapter = new TQueueAdaptor(&realQueue);
                    adapters.emplace_back(adapter);
                    auto action = [queue = adapter] () -> bool {
                        if (!queue->TryPop()) {
                            if constexpr (Sleep) {
                                NanoSleep(1'000);
                            } else {
                                SpinLockPause();
                            }
                        }
                        return true;
                    };
                    TWorkerWithDuration<std::decay_t<decltype(action)>> worker(action, duration);
                    threads.emplace_back(new TTestThread<decltype(worker), TStatsCollector>(worker, &collector));
                }
                RunThreads(threads);
                return std::move(collector);
            }

            TString Validate(ui64 globalThreads) override {
                if (globalThreads < 2) {
                    return TStringBuilder() << "A number greater than 1 was expected instead of " << globalThreads;
                }
                return "";
            }

            std::variant<TThreadCounts, ui64> GetThreads(ui64 globalThreads) override {
                return TThreadCounts{globalThreads - 1, 1};
            }
        };

    };

}
