#include "actorsystem.h"
#include "actor_bootstrapped.h"
#include "config.h"
#include "executor_pool_basic.h"
#include "hfunc.h"
#include "scheduler_basic.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/thread/pool.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <iostream>
#include <memory>
#include <mutex>
#include <span>
#include <utility>
#include <unordered_set>
#include <vector>

#define BENCH_START(label)  auto label##Start = std::chrono::steady_clock::now()
#define BENCH_END(label) std::chrono::steady_clock::now() - label##Start

using namespace NActors;
using namespace std::chrono_literals;

Y_UNIT_TEST_SUITE(ActorSystemBenchmark) {

    enum ESimpleEventType {
        EvQuickSort,
        EvActorStatus,
        EvSumVector,
        EvSumVectorResult,
        EvSumSendRequests,
    };

    using TContainer = std::vector<i32>;
    using TActorIds = std::span<TActorId>;

    template <typename TId>
    class TActiveEntityRegistry {
    private:
        std::unordered_set<TId> ActiveIds_;
        std::mutex ActiveIdsMutex_;
        std::condition_variable ActiveIdsCv_;

    public:
        void SetActive(TId id) {
            std::unique_lock lock(ActiveIdsMutex_);
            ActiveIds_.insert(std::move(id));
        }

        void SetInactive(const TId& id) {
            std::unique_lock lock(ActiveIdsMutex_);
            ActiveIds_.erase(id);
            ActiveIdsCv_.notify_all();
        }

        bool WaitForAllInactive(std::chrono::microseconds timeout = 1ms) {
            std::unique_lock lock(ActiveIdsMutex_);
            ActiveIdsCv_.wait_for(lock, timeout, [this] {
                return ActiveIds_.empty();
            });
            return ActiveIds_.empty();
        }
    };

    class TQuickSortEngine {
    public:
        struct TParameters {
            TContainer &Container;
            i32 Left;
            i32 Right;
            void* CustomData = nullptr;

            TParameters() = delete;
            TParameters(TContainer& container, i32 left, i32 right, void* customData)
                : Container(container)
                , Left(left)
                , Right(right)
                , CustomData(customData)
            {}
        };

    public:
        void Sort(TQuickSortEngine::TParameters& params) {
            auto [newRight, newLeft] = Partition(params.Container, params.Left, params.Right);
            if (!(params.Left < newRight || newLeft < params.Right)) {
                return;
            }

            auto [leftParams, rightParams] = SplitParameters(params, newRight, newLeft);

            bool ranAsync = false;

            if (newLeft < params.Right) {
                ranAsync = TryRunAsync(rightParams);
                if (ranAsync) {
                    Sort(rightParams);
                }
            }
            if (params.Left < newRight) {
                if (!ranAsync && !TryRunAsync(leftParams)) {
                    Sort(leftParams);
                }
            }
        }

        // returns bounds of left and right sub-arrays for the next iteration
        std::pair<i32, i32> Partition(TContainer& container, i32 left, i32 right) {
            ui32 pivotIndex = (left + right) / 2;
            auto pivot = container[pivotIndex];
            while (left <= right) {
                while (container[left] < pivot) {
                    left++;
                }
                while (container[right] > pivot) {
                    right--;
                }

                if (left <= right) {
                    std::swap(container[left++], container[right--]);
                }
            }
            return {right, left};
        }

    protected:
        virtual std::pair<TParameters, TParameters> SplitParameters(const TParameters& params, i32 newRight, i32 newLeft) = 0;
        virtual bool TryRunAsync(const TParameters& params) = 0;
    };

    class TQuickSortTask : public TQuickSortEngine, public IObjectInQueue {
    public:
        using TParameters = TQuickSortEngine::TParameters;
        struct TThreadPoolParameters {
            const ui32 ThreadsLimit = 0;
            std::atomic<ui32> ThreadsUsed = 0;
            TThreadPool& ThreadPool;

            TThreadPoolParameters(ui32 threadsLimit, ui32 threadsUsed, TThreadPool &threadPool)
                : ThreadsLimit(threadsLimit)
                , ThreadsUsed(threadsUsed)
                , ThreadPool(threadPool)
            {}
        };
        TParameters Params;
        TActiveEntityRegistry<TQuickSortTask*>& ActiveThreadRegistry;

    public:
        TQuickSortTask() = delete;
        TQuickSortTask(TParameters params, TActiveEntityRegistry<TQuickSortTask*>& activeThreadRegistry)
            : Params(params)
            , ActiveThreadRegistry(activeThreadRegistry)
        {
            ActiveThreadRegistry.SetActive(this);
        }

        void Process(void*) override {
            Sort(Params);
            ActiveThreadRegistry.SetInactive(this);
        }

    protected:
        std::pair<TParameters, TParameters> SplitParameters(const TParameters& params, i32 newRight, i32 newLeft) override {
            return {
                {params.Container, params.Left, newRight, params.CustomData},
                {params.Container, newLeft, params.Right, params.CustomData}
            };
        }

        bool TryRunAsync(const TParameters& params) override {
            auto threadPoolParams = static_cast<TThreadPoolParameters*>(params.CustomData);
            if (threadPoolParams->ThreadsUsed++ >= threadPoolParams->ThreadsLimit) {
                threadPoolParams->ThreadsUsed--;
                return false;
            }
            return threadPoolParams->ThreadPool.AddAndOwn(THolder(new TQuickSortTask(params, ActiveThreadRegistry)));
        }
    };

    class TEvQuickSort : public TEventLocal<TEvQuickSort, EvQuickSort> {
    public:
        using TParameters = TQuickSortEngine::TParameters;
        struct TActorSystemParameters {
            TActorIds ActorIds;
            std::atomic<ui32> ActorIdsUsed = 0;
            TActorSystemParameters() = delete;
            TActorSystemParameters(const TActorIds& actorIds, ui32 actorIdsUsed = 0)
                : ActorIds(actorIds)
                , ActorIdsUsed(actorIdsUsed)
            {}
        };

        TQuickSortEngine::TParameters Params;

    public:
        TEvQuickSort() = delete;
        TEvQuickSort(TParameters params, TActiveEntityRegistry<TEvQuickSort*>& activeEventRegistry)
            : Params(params)
            , ActiveEventRegistry_(activeEventRegistry)
        {
            Y_VERIFY(!Params.Container.empty());
            Y_VERIFY(Params.Right - Params.Left + 1 <= static_cast<i32>(Params.Container.size()),
                "left: %d, right: %d, cont.size: %d", Params.Left, Params.Right, static_cast<i32>(Params.Container.size()));
            ActiveEventRegistry_.SetActive(this);
        }

        virtual ~TEvQuickSort() {
            ActiveEventRegistry_.SetInactive(this);
        }

    private:
        TActiveEntityRegistry<TEvQuickSort*>& ActiveEventRegistry_;
    };

    class TQuickSortActor : public TQuickSortEngine, public TActorBootstrapped<TQuickSortActor> {
    public:
        using TParameters = TQuickSortEngine::TParameters;

    private:
        TActiveEntityRegistry<TEvQuickSort*>& ActiveEventRegistry_;

    public:
        TQuickSortActor() = delete;
        TQuickSortActor(TActiveEntityRegistry<TEvQuickSort*>& activeEventRegistry)
            : TActorBootstrapped<TQuickSortActor>()
            , ActiveEventRegistry_(activeEventRegistry)
        {}

        STFUNC(StateInit) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvQuickSort, Handle);
                default:
                    Y_VERIFY(false);
            }
        }

        void Bootstrap() {
            Become(&TThis::StateInit);
        }

    protected:
        std::pair<TParameters, TParameters> SplitParameters(const TParameters& params, i32 newRight, i32 newLeft) override {
            return {
                {params.Container, params.Left, newRight, params.CustomData},
                {params.Container, newLeft, params.Right, params.CustomData}
            };
        }

        bool TryRunAsync(const TParameters& params) override {
            auto actorSystemParams = static_cast<TEvQuickSort::TActorSystemParameters*>(params.CustomData);
            const auto actorIdIndex = actorSystemParams->ActorIdsUsed++;
            if (actorIdIndex >= actorSystemParams->ActorIds.size()) {
                actorSystemParams->ActorIdsUsed--;
                return false;
            }
            auto targetActorId = actorSystemParams->ActorIds[actorIdIndex];
            Send(targetActorId, new TEvQuickSort(params, ActiveEventRegistry_));
            return true;
        }

    private:
        void Handle(TEvQuickSort::TPtr& ev) {
            auto evPtr = ev->Get();
            Sort(evPtr->Params);
        }
    };

    std::vector<i32> PrepareVectorToSort(ui32 n) {
        std::vector<i32> numbers(n);
        for (ui32 i = 0; i < numbers.size(); i++) {
            numbers[i] = numbers.size() - i;
        }
        return numbers;
    }

    std::unique_ptr<TActorSystem> PrepareActorSystem(ui32 poolThreads, TAffinity* affinity = nullptr) {
        auto setup = MakeHolder<TActorSystemSetup>();
        setup->NodeId = 1;

        setup->ExecutorsCount = 1;
        setup->Executors.Reset(new TAutoPtr<IExecutorPool>[setup->ExecutorsCount]);

        ui32 poolId = 0;
        ui64 poolSpinThreashold = 20;
        setup->Executors[0].Reset(new TBasicExecutorPool(
            poolId, poolThreads, poolSpinThreashold, "", nullptr, affinity));
        TSchedulerConfig schedulerConfig;
        schedulerConfig.ResolutionMicroseconds = 512;
        schedulerConfig.SpinThreshold = 100;
        setup->Scheduler.Reset(new TBasicSchedulerThread(schedulerConfig));

        return std::make_unique<TActorSystem>(setup);
    }

    std::vector<TActorId> prepareQuickSortActors(
        TActorSystem* actorSystem, ui32 actorsNum, TActiveEntityRegistry<TEvQuickSort*>& activeEventRegistry
    ) {
        std::vector<TActorId> actorIds;
        actorIds.reserve(actorsNum);

        for (ui32 i = 0; i < actorsNum; i++) {
            auto actor = new TQuickSortActor(activeEventRegistry);
            auto actorId = actorSystem->Register(actor);
            actorIds.push_back(actorId);
        }

        return actorIds;
    }

    std::pair<std::chrono::microseconds, std::vector<i32>> BenchmarkQuickSortActor(
        ui32 threads,
        ui32 iterations,
        ui32 vectorSize
    ) {
        auto actorSystem = PrepareActorSystem(threads);
        actorSystem->Start();

        TActiveEntityRegistry<TEvQuickSort*> activeEventRegistry;
        auto actorIds = prepareQuickSortActors(actorSystem.get(), threads, activeEventRegistry);

        std::vector<i32> actorSortResult;
         auto actorQsDurationTotal = 0us;
        for (ui32 i = 0; i < iterations; i++) {
            auto numbers = PrepareVectorToSort(vectorSize);

            TEvQuickSort::TActorSystemParameters actorSystemParams(actorIds, 1);
            TEvQuickSort::TParameters params(numbers, 0, numbers.size() - 1, &actorSystemParams);
            auto ev3 = new TEvQuickSort(params, activeEventRegistry);

            BENCH_START(qs);

            actorSystem->Send(actorIds.front(), ev3);
            UNIT_ASSERT_C(activeEventRegistry.WaitForAllInactive(60s), "timeout");

            actorQsDurationTotal += std::chrono::duration_cast<std::chrono::microseconds>(BENCH_END(qs));

            if (i + 1 == iterations) {
                actorSortResult = numbers;
            }
        }

        return {actorQsDurationTotal / iterations, actorSortResult};
    }

    std::pair<std::chrono::microseconds, std::vector<i32>> BenchmarkQuickSortThreadPool(
        ui32 threads,
        ui32 iterations,
        ui32 vectorSize
    ) {
        TThreadPool threadPool;
        threadPool.Start(threads);
        TActiveEntityRegistry<TQuickSortTask*> activeThreadRegistry;

        auto threaPoolSortDurationTotal = 0us;
        std::vector<i32> threadPoolSortResult;
        for (ui32 i = 0; i < iterations; i++) {
            auto numbers = PrepareVectorToSort(vectorSize);

            TQuickSortTask::TThreadPoolParameters threadPoolParams(threads, 1, threadPool);
            TQuickSortTask::TParameters params(numbers, 0, numbers.size() - 1, &threadPoolParams);

            BENCH_START(thread);

            Y_VERIFY(threadPoolParams.ThreadPool.AddAndOwn(THolder(new TQuickSortTask(params, activeThreadRegistry))));
            UNIT_ASSERT_C(activeThreadRegistry.WaitForAllInactive(60s), "timeout");

            threaPoolSortDurationTotal += std::chrono::duration_cast<std::chrono::microseconds>(BENCH_END(thread));

            if (i + 1 == iterations) {
                threadPoolSortResult = numbers;
            }
        }

        threadPool.Stop();

        return {threaPoolSortDurationTotal / iterations, threadPoolSortResult};
    }

    Y_UNIT_TEST(QuickSortActor) {
        const std::vector<ui32> threadss{1, 4};
        const std::vector<ui32> vectorSizes{100, 1'000, 1'000'000};
        const ui32 iterations = 3;

        std::cout << "sep=," << std::endl;
        std::cout << "size,threads,actor_time(us),thread_pool_time(us)" << std::endl;

        for (auto vectorSize : vectorSizes) {
            for (auto threads : threadss) {
                std::cerr << "vector size: " << vectorSize << ", threads: " << threads << std::endl;

                auto [actorSortDuration, actorSortResult] = BenchmarkQuickSortActor(threads, iterations, vectorSize);
                std::cerr << "actor sort duration: " << actorSortDuration.count() << "us" << std::endl;

                auto [threadPoolSortDuration, threadPoolSortResult] = BenchmarkQuickSortThreadPool(threads, iterations, vectorSize);
                std::cerr << "thread pool sort duration: " << threadPoolSortDuration.count() << "us" << std::endl;

                auto referenceVector = PrepareVectorToSort(vectorSize);
                std::sort(referenceVector.begin(), referenceVector.end());

                UNIT_ASSERT_EQUAL_C(actorSortResult, referenceVector,
                    "vector size: " << vectorSize << "; threads: " << threads);
                UNIT_ASSERT_EQUAL_C(threadPoolSortResult, referenceVector,
                    "vector size: " << vectorSize << "; threads: " << threads);

                std::cout << vectorSize << ","
                          << threads << ","
                          << actorSortDuration.count() << ","
                          << threadPoolSortDuration.count() << std::endl;
            }

            std::cerr << "-----" << std::endl << std::endl;
        }
    }

    // vector sum benchmark

    i64 CalculateOddSum(const TContainer& numbers) {
        i64 result = 0;
        for (auto x : numbers) {
            if (x % 2 == 1) {
                result += x;
            }
        }

        return result;
    }

    TContainer prepareVectorToSum(const ui32 vectorSize) {
        TContainer numbers;
        numbers.reserve(vectorSize);
        for (ui32 i = 0; i < vectorSize; i++) {
            numbers.push_back(i + 1);
        }

        return numbers;
    }

    class TEvSumVector : public TEventLocal<TEvSumVector, EvSumVector> {
    public:
        const TContainer Numbers;

    public:
        TEvSumVector() = delete;

        TEvSumVector(TContainer&& numbers)
            : Numbers(std::move(numbers))
        {}
    };

    class TEvSumVectorResult : public TEventLocal<TEvSumVectorResult, EvSumVectorResult> {
    public:
        const i64 Sum = 0;

    public:
        TEvSumVectorResult(i64 sum)
            : Sum(sum)
        {}
    };

    class TEvSumSendRequests : public TEventLocal<TEvSumSendRequests, EvSumSendRequests> {
    public:
        const ui32 VectorSize;
        const ui32 RequestsNumber;
        const TActorIds ActorIds;

    public:
        TEvSumSendRequests() = delete;
        TEvSumSendRequests(ui32 vectorSize, ui32 requestsNumber, TActorIds actorIds)
            : VectorSize(vectorSize)
            , RequestsNumber(requestsNumber)
            , ActorIds(actorIds)
        {}
    };

    class TSumProxyActor : public TActorBootstrapped<TSumProxyActor> {
    private:
        i64 LastSum_ = 0;
        ui32 NumberOfResults_ = 0;
        ui32 ExpectedResults_ = 0;
        ui32 VectorSize_ = 0;
        TActorIds SumVectorActorIds_ = {};
        ui32 LastUsedActor_ = 0;

        std::mutex NumberOfResultsMutex_;
        std::condition_variable NumberOfResultsCv_;

    public:
        STFUNC(StateInit) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvSumSendRequests, HandleRequest);
                hFunc(TEvSumVectorResult, HandleResult);
                default:
                    Y_VERIFY(false);
            }
        }

        void Bootstrap() {
            Become(&TThis::StateInit);
        }

        i64 LastSum() {
            return LastSum_;
        }

        bool WaitForResults(std::chrono::microseconds timeout = 1ms, bool nonZero = true) {
            std::unique_lock lock(NumberOfResultsMutex_);
            NumberOfResultsCv_.wait_for(lock, timeout, [this, nonZero] {
                return ((nonZero && NumberOfResults_ != 0) || !nonZero)
                    && NumberOfResults_ == ExpectedResults_;
            });
            return NumberOfResults_ == ExpectedResults_;
        }

        void ShiftLastUsedActor(ui32 shift) {
            LastUsedActor_ += shift;
        }

    private:
        TActorId NextActorId() {
            auto actorId = SumVectorActorIds_[LastUsedActor_ % SumVectorActorIds_.size()];
            LastUsedActor_ = (LastUsedActor_ + 1) % SumVectorActorIds_.size();

            return actorId;
        }

        bool SendVectorIfNeeded() {
            if (NumberOfResults_ < ExpectedResults_) {
                Send(NextActorId(), new TEvSumVector(prepareVectorToSum(VectorSize_)));
                return true;
            }
            return false;
        }

        void HandleRequest(TEvSumSendRequests::TPtr& ev) {
            auto evPtr = ev->Get();
            ExpectedResults_ = evPtr->RequestsNumber;
            VectorSize_ = evPtr->VectorSize;
            SumVectorActorIds_ = evPtr->ActorIds;

            {
                std::unique_lock lock(NumberOfResultsMutex_);
                NumberOfResults_ = 0;

                SendVectorIfNeeded();
            }
        }

        void HandleResult(TEvSumVectorResult::TPtr& ev) {
            LastSum_ = ev->Get()->Sum;
            {
                std::unique_lock lock(NumberOfResultsMutex_);
                NumberOfResults_++;

                if (!SendVectorIfNeeded()) {
                    NumberOfResultsCv_.notify_all();
                }
            }
        }
    };

    class TSumVectorActor : public TActorBootstrapped<TSumVectorActor> {
    private:
        TActorId ResultActorId_;

    public:
        STFUNC(StateInit) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvSumVector, Handle);
                default:
                    Y_VERIFY(false);
            }
        }

        void Bootstrap() {
            Become(&TThis::StateInit);
        }

    private:
        void Handle(TEvSumVector::TPtr& ev) {
            auto evPtr = ev->Get();
            auto oddSum = CalculateOddSum(evPtr->Numbers);

            Send(ev->Sender, new TEvSumVectorResult(oddSum));
        }
    };

    std::vector<TActorId> prepareSumActors(TActorSystem* actorSystem, ui32 actorsNumber) {
        std::vector<TActorId> actorIds;
        actorIds.reserve(actorsNumber);
        for (ui32 i = 0; i < actorsNumber; i++) {
            actorIds.push_back(actorSystem->Register(new TSumVectorActor()));
        }
        return actorIds;
    }

    std::pair<std::vector<TSumProxyActor*>, std::vector<TActorId>> prepareProxyActors(
        TActorSystem* actorSystem, ui32 actorsNumber
    ) {
        std::pair<std::vector<TSumProxyActor*>, std::vector<TActorId>> result;
        auto& [actors, actorIds] = result;
        actors.reserve(actorsNumber);
        actorIds.reserve(actorsNumber);
        for (ui32 i = 0; i < actorsNumber; i++) {
            actors.push_back(new TSumProxyActor());
            actorIds.push_back(actorSystem->Register(actors.back()));
            actors.back()->ShiftLastUsedActor(i);
        }

        return result;
    }

    std::chrono::microseconds calcTimeoutForSumVector(
        ui32 vectorSize, ui32 iterations, ui32 proxyActorsNum, ui32 sumActorsNum, ui32 threadsNum
    ) {
        auto expectedMaxTimePerMillion = 100000us;
        auto vectorSizeRatio = vectorSize / 1000000 + 1;

        return expectedMaxTimePerMillion * vectorSizeRatio * iterations * proxyActorsNum / std::min(threadsNum, sumActorsNum);
    }

    bool WaitForSumActorResult(const std::vector<TSumProxyActor*>& actors, std::chrono::microseconds timeout = 1ms) {
        for (auto& actor : actors) {
            if (!actor->WaitForResults(timeout)) {
                return false;
            }
        }
        return true;
    }

    std::pair<std::chrono::microseconds, i64> BenchmarkSumVectorActor(
        ui32 threads,
        ui32 proxyActorsNumber,
        ui32 sumActorsNumber,
        ui32 iterations,
        ui32 vectorSize
    ) {
        auto actorSystem = PrepareActorSystem(threads);
        actorSystem->Start();

        auto sumActorIds = prepareSumActors(actorSystem.get(), sumActorsNumber);
        auto [proxyActors, proxyActorIds] = prepareProxyActors(actorSystem.get(), proxyActorsNumber);

        auto timeout = calcTimeoutForSumVector(vectorSize, iterations, proxyActorsNumber, sumActorsNumber, threads);

        BENCH_START(sumVectorActor);

        for (auto proxyActorId : proxyActorIds) {
            actorSystem->Send(proxyActorId, new TEvSumSendRequests(vectorSize, iterations, sumActorIds));
        }

        UNIT_ASSERT_C(WaitForSumActorResult(proxyActors, timeout), "timeout");

        auto totalDuration = std::chrono::duration_cast<std::chrono::microseconds>(BENCH_END(sumVectorActor));
        auto checkSum = proxyActors.back()->LastSum();

        return {totalDuration / iterations, checkSum};
    }

    Y_UNIT_TEST(SumVector) {
        using TVui64 = std::vector<ui64>;
        const bool forCI = true;
        const TVui64 vectorSizes = forCI ?
            TVui64{1'000, 1'000'000} : TVui64{1'000, 1'000'000, 10'000'000, 100'000'000};
        const TVui64 threadsNumbers = forCI ? TVui64{1} : TVui64{1, 4};
        const TVui64 proxyActorsNumbers = forCI ? TVui64{1} : TVui64{1, 4};
        const TVui64 sumActorsNumbers = forCI ? TVui64{1} : TVui64{1, 8, 32};
        const ui32 iterations = 30;

        std::cout << "sep=," << std::endl;
        std::cout << "size,threads,proxy_actors,sum_actors,duration(us)" << std::endl;

        for (auto vectorSize : vectorSizes) {
            for (auto threads : threadsNumbers) {
                for (auto proxyActors : proxyActorsNumbers) {
                    for (auto sumActors : sumActorsNumbers) {
                        std::cerr << "vector size: " << vectorSize
                                  << ", threads: " << threads
                                  << ", proxy actors: " << proxyActors
                                  << ", sum actors: " << sumActors << std::endl;

                        auto [duration, resultSum] = BenchmarkSumVectorActor(
                            threads, proxyActors, sumActors, iterations, vectorSize);
                        std::cerr << "duration: " << duration.count() << "us" << std::endl;

                        const i64 referenceSum = vectorSize * vectorSize / 4;
                        UNIT_ASSERT_EQUAL_C(
                            resultSum, referenceSum,
                            resultSum << "!=" << referenceSum << "; failed on vectorSize=" << vectorSize
                            << ", threads=" << threads
                            << ", proxyActors=" << proxyActors
                            << ", sumActors=" << sumActors);

                        std::cout << vectorSize << ","
                                  << threads << ","
                                  << proxyActors << ","
                                  << sumActors << ","
                                  << duration.count()
                                  << std::endl;
                    }
                }
            }
        }
    }
}
