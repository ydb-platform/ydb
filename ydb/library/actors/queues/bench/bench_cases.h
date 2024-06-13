#pragma once

#include "queue.h"
#include "worker.h"


namespace NActors::NQueueBench {
    
    template <typename TRealQueue, typename TQueueAdaptor>
    struct TBenchCases {

        template <typename TStatsCollector>
        static TStatsCollector BasicPushPopSingleThread() {
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

        template <typename TStatsCollector, ui32 ThreadCount, ui32 RepeatCount>
        static TStatsCollector BasicPushPopMultiThreads() {
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

        template <typename TStatsCollector, ui32 ThreadCount>
        static TStatsCollector BasicProducing() {
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

        template <typename TStatsCollector, ui32 ThreadCount, ui32 Reads>
        static TStatsCollector ConsumingEmptyQueue() {
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

        template <typename TStatsCollector, ui32 ProducingThreadCount, ui32 ConsumingThreadCount, ui32 PushedItems, ui32 PoppedItems>
        static TStatsCollector BasicProducingConsuming() {
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

    };

}
