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
#include <optional>
#include <span>
#include <string>
#include <thread>
#include <utility>
#include <unordered_set>
#include <unordered_map>
#include <vector>

#define BENCH_START(label)  auto label##Start = std::chrono::steady_clock::now()
#define BENCH_END(label) std::chrono::steady_clock::now() - label##Start

using namespace NActors;
using namespace std::chrono_literals;

Y_UNIT_TEST_SUITE(ActorSystemBenchmark) {

    enum ESimpleEventType {
        EvQuickSort,
        EvSumVector,
        EvSumVectorResult,
        EvSumSendRequests,
        EvKvSearch,
        EvKvSendRequests,
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
            if (ActiveIds_.empty()) {
                ActiveIdsCv_.notify_all();
            }
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
            Y_ABORT_UNLESS(!Params.Container.empty());
            Y_ABORT_UNLESS(Params.Right - Params.Left + 1 <= static_cast<i32>(Params.Container.size()),
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
                    Y_ABORT_UNLESS(false);
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

            Y_ABORT_UNLESS(threadPool.AddAndOwn(THolder(new TQuickSortTask(params, activeThreadRegistry))));
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

    // KV-storage benchmark

    using TKvKey = std::string;
    using TKvValue = i32;
    using TDict = std::unordered_map<TKvKey, TKvValue>;

    struct TSearchStat {
        ui32 Found = 0;
        ui32 NotFound = 0;

        bool operator==(const TSearchStat& other) {
            return Found == other.Found && NotFound == other.NotFound;
        }
    };

    class TKvSearchTask : public IObjectInQueue {
    private:
        TKvKey Key_;
        const TDict& Dict_;
        TSearchStat SearchStat_ = {};

    public:
        TKvSearchTask() = delete;
        TKvSearchTask(TKvKey key, const TDict& dict)
            : Key_(key)
            , Dict_(dict)
        {}

        void Process(void*) override {
            if (Dict_.contains(Key_)) {
                SearchStat_.Found++;
            } else {
                SearchStat_.NotFound++;
            }
        }
    };

    class TEvKvSearch : public TEventLocal<TEvKvSearch, EvKvSearch> {
    public:
        TKvKey Key;

    public:
        TEvKvSearch() = delete;
        TEvKvSearch(TKvKey key)
            : Key(std::move(key))
        {}
    };

    class TEvKvSendRequests : public TEventLocal<TEvKvSendRequests, EvKvSendRequests> {
    public:
        const std::vector<std::string>& KeysToSearch;
        const std::vector<TActorId> SearchActorIds;

    public:
        TEvKvSendRequests() = delete;
        TEvKvSendRequests(const std::vector<std::string>& keysToSearch, std::vector<TActorId>&& searchActorIds)
            : KeysToSearch(keysToSearch)
            , SearchActorIds(std::move(searchActorIds))
        {}
    };

    class TKvSendRequestActor : public TActorBootstrapped<TKvSendRequestActor> {
    public:
        STFUNC(StateInit) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvKvSendRequests, Handle);
                default:
                    Y_ABORT_UNLESS(false);
            }
        }

        void Bootstrap() {
            Become(&TThis::StateInit);
        }

    private:
        void Handle(TEvKvSendRequests::TPtr& ev) {
            auto evPtr = ev->Get();
            ui32 actorIdx = 0;
            for (auto& key : evPtr->KeysToSearch) {
                auto actorId = evPtr->SearchActorIds[actorIdx];
                actorIdx = (actorIdx + 1) % evPtr->SearchActorIds.size();

                Send(actorId, new TEvKvSearch(key));
            }
        }
    };

    class TKvSearchActor : public TActorBootstrapped<TKvSearchActor> {
    private:
        const TDict& Dict_;
        TSearchStat SearchStat_ = {};
        std::atomic<ui32> CompletedEvents_ = 0;

    public:
        TKvSearchActor() = delete;
        TKvSearchActor(const TDict& dict)
            : TActorBootstrapped<TKvSearchActor>()
            , Dict_(dict)
        {}

        STFUNC(StateInit) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvKvSearch, Handle);
                default:
                    Y_ABORT_UNLESS(false);
            }
        }

        void Bootstrap() {
            Become(&TThis::StateInit);
        }

        const TSearchStat& SearchStat() {
            return SearchStat_;
        }

        ui32 CompletedEvents() {
            return CompletedEvents_;
        }
    private:
        void Handle(TEvKvSearch::TPtr& ev) {
            auto evPtr = ev->Get();

            if (Dict_.contains(evPtr->Key)) {
                SearchStat_.Found++;
            } else {
                SearchStat_.NotFound++;
            }
            CompletedEvents_++;
        }
    };

    TDict prepareKvSearchDict(const i32 dictSize) {
        std::string permutableString = "abcdefghijklm";
        TDict dict;
        for (i32 i = 0; i < dictSize; i++) {
            dict.emplace(permutableString, i);
            std::next_permutation(permutableString.begin(), permutableString.end());
        }

        return dict;
    }

    std::vector<std::string> prepareKeysToSearch(const TDict &dict, ui32 requestsNumber) {
        std::vector<std::string> keys;
        auto keyAppearances = requestsNumber / dict.size() + 1;
        keys.reserve(keyAppearances * dict.size());

        for (auto& [key, _] : dict) {
            for (ui32 i = 0; i < keyAppearances; i++) {
                keys.push_back(key);

                // keep the original key value to search
                if (i % 4 == 0) {
                    continue;
                }

                // make non-exising key
                keys.back() += "nonexistingkey";
            }
        }

        Y_ABORT_UNLESS(keys.size() >= requestsNumber);

        std::random_shuffle(keys.begin(), keys.end());
        keys.resize(requestsNumber);

        return keys;
    }

    std::pair<std::vector<TKvSearchActor*>, std::vector<TActorId>> prepareKvSearchActors(
        TActorSystem* actorSystem, ui32 searchActorsNum, const std::vector<TDict>& dicts
    ) {
        std::vector<TKvSearchActor*> searchActors;
        std::vector<TActorId> searchActorIds;
        searchActors.reserve(searchActorsNum);
        searchActorIds.reserve(searchActorsNum);
        for (ui32 i = 0, dictIdx = 0; i < searchActorsNum; i++) {
            const auto& dict = dicts[dictIdx];
            dictIdx = (dictIdx + 1) % dicts.size();

            auto kvSearchActor = new TKvSearchActor(dict);
            auto kvSearchActorId = actorSystem->Register(kvSearchActor);
            searchActors.push_back(kvSearchActor);
            searchActorIds.push_back(kvSearchActorId);
        }

        return {searchActors, searchActorIds};
    }

    ui32 CalculateCompletedEvents(const std::vector<TKvSearchActor*>& actors) {
        ui32 completedEvents = 0;
        for (auto actor : actors) {
            completedEvents += actor->CompletedEvents();
        }
        return completedEvents;
    }

    TSearchStat CollectKvSearchActorStat(const std::vector<TKvSearchActor*>& actors) {
        TSearchStat stat;
        for (auto actor : actors) {
            stat.Found += actor->SearchStat().Found;
            stat.NotFound += actor->SearchStat().NotFound;
        }
        return stat;
    }

    std::pair<std::chrono::microseconds, TSearchStat> BenchmarkKvActor(
        ui32 threads, ui32 actors, ui32 iterations, const std::vector<TDict>& dicts, const std::vector<std::string>& keysToSearch
    ) {
        TSearchStat stat = {};
        auto kvSearchActorDuration = 0us;

        for (ui32 i = 0; i < iterations; i++) {
            auto actorSystem = PrepareActorSystem(threads);
            actorSystem->Start();

            auto [kvSearchActors, kvSearchActorIds] = prepareKvSearchActors(actorSystem.get(), actors, dicts);

            auto kvSendRequestActorId = actorSystem->Register(new TKvSendRequestActor());

            BENCH_START(kvSearch);
            actorSystem->Send(kvSendRequestActorId, new TEvKvSendRequests(keysToSearch, std::move(kvSearchActorIds)));

            // CondVar logic gives too much of overhead (2-10 times more than just sleep_for)
            while (CalculateCompletedEvents(kvSearchActors) < keysToSearch.size()) {
                std::this_thread::sleep_for(1us);
            }

            kvSearchActorDuration += std::chrono::duration_cast<std::chrono::microseconds>(BENCH_END(kvSearch));

            if (i + 1 == iterations) {
                stat = CollectKvSearchActorStat(kvSearchActors);
            }
        }

        return {kvSearchActorDuration / iterations, stat};
    }

    std::pair<std::chrono::microseconds, TSearchStat> BenchmarkKvActorExternalSender(
        ui32 threads, ui32 actors, ui32 iterations, const std::vector<TDict>& dicts, const std::vector<std::string>& keysToSearch
    ) {
        TSearchStat stat = {};
        auto kvSearchActorDuration = 0us;
        for (ui32 i = 0; i < iterations; i++) {
            auto actorSystem = PrepareActorSystem(threads);
            actorSystem->Start();

            auto [kvSearchActors, kvSearchActorIds] = prepareKvSearchActors(actorSystem.get(), actors, dicts);

            BENCH_START(kvSearch);
            ui32 actorIdToUseIndex = 0;
            for (auto& key : keysToSearch) {
                actorSystem->Send(kvSearchActorIds[actorIdToUseIndex], new TEvKvSearch(key));
                actorIdToUseIndex = (actorIdToUseIndex + 1) % kvSearchActorIds.size();
            }

            // CondVar logic gives too much of overhead (2-10 times more than just sleep_for)
            while (CalculateCompletedEvents(kvSearchActors) < keysToSearch.size()) {
                std::this_thread::sleep_for(1us);
            }

            kvSearchActorDuration += std::chrono::duration_cast<std::chrono::microseconds>(BENCH_END(kvSearch));

            if (i + 1 == iterations) {
                stat = CollectKvSearchActorStat(kvSearchActors);
            }
        }

        return {kvSearchActorDuration / iterations, stat};
    }

    std::chrono::microseconds BenchmarkKvThreadPool(
        ui32 threads, ui32 iterations, const TDict& dict, const std::vector<std::string>& keysToSearch
    ) {
        TThreadPool threadPool;

        auto kvSearchActorDuration = 0us;
        for (ui32 i = 0; i < iterations; i++) {
            threadPool.Start(threads);

            BENCH_START(kvSearch);

            for (auto& key : keysToSearch) {
                Y_ABORT_UNLESS(threadPool.AddAndOwn(THolder(new TKvSearchTask(key, dict))));
            }

            // CondVar logic gives too much of overhead (2-10 times more than just sleep_for)
            while (threadPool.Size() > 0) {
                std::this_thread::sleep_for(1us);
            }
            threadPool.Stop();

            kvSearchActorDuration += std::chrono::duration_cast<std::chrono::microseconds>(BENCH_END(kvSearch));
        }

        return {kvSearchActorDuration / iterations};
    }

    std::pair<std::chrono::microseconds, TSearchStat> BenchmarkKvSingleThread(
        ui32 iterations, const TDict& dict, const std::vector<std::string>& keysToSearch
    ) {
        TSearchStat stat = {};
        auto kvSearchDuration = 0us;
        for (ui32 i = 0; i < iterations; i++) {
            TSearchStat iterationStat = {};
            BENCH_START(kvSearch);
            for (auto& key : keysToSearch) {
                if (dict.contains(key)) {
                    iterationStat.Found++;
                } else {
                    iterationStat.NotFound++;
                }
            }
            kvSearchDuration += std::chrono::duration_cast<std::chrono::microseconds>(BENCH_END(kvSearch));

            if (i + 1 == iterations) {
                stat = iterationStat;
            }
        }

        return {kvSearchDuration / iterations, stat};
    }

    Y_UNIT_TEST(KvActor) {
        const bool forCI = true;

        using TNumbers = std::vector<ui32>;
        const TNumbers threadNumbers = forCI ? TNumbers{1} : TNumbers{1, 4, 8};
        const TNumbers actorNumbers = forCI ? TNumbers{1, 8} : TNumbers{1, 4, 8, 16, 32, 64};
        const TNumbers dictSizes = forCI ? TNumbers{1'000} : TNumbers{1'000, 1'000'000};
        const TNumbers dictsNumbers = forCI ? TNumbers{1} : TNumbers{1, 8};
        const ui32 iterations = 5;

        std::cout << "sep=," << std::endl;
        std::cout << "requests_number,dicts_number,dict_size,threads,actors,actor_time(us),actor_ext_time(us),thread_pool_time(us),single_thread_time(us)" << std::endl;

        for (auto dictsNumber : dictsNumbers) {
            for (auto dictSize : dictSizes) {
                const auto dict = prepareKvSearchDict(dictSize);
                const ui32 requestsNumber = forCI ? 10'000 : 1'000'000;
                const auto keysToSearch = prepareKeysToSearch(dict, requestsNumber);

                for (auto threads : threadNumbers) {
                    std::cerr << "requestsNumber: " << requestsNumber
                            << ", dictSize: " << dictSize
                            << ", threads: " << threads << std::endl;

                    auto tpKvDuration = BenchmarkKvThreadPool(threads, iterations, dict, keysToSearch);
                    std::cerr << "kv search threadpool duration: " << tpKvDuration.count() << "us" << std::endl;

                    auto [singleThreadKvDuration, singleThreadKvStat] = BenchmarkKvSingleThread(iterations, dict, keysToSearch);
                    std::cerr << "kv search single thread duration: " << singleThreadKvDuration.count() << "us" << std::endl;

                    std::vector<TDict> dicts(dictsNumber, dict);

                    for (auto actors : actorNumbers) {
                        std::cerr << "----" << std::endl
                                << "requestsNumber: " << requestsNumber
                                << ", dictsNumber: " << dictsNumber
                                << ", dictSize: " << dictSize
                                << ", threads: " << threads
                                << ", actors: " << actors << std::endl;

                        auto [actorKvDuration, actorKvStat] = BenchmarkKvActor(threads, actors, iterations, dicts, keysToSearch);
                        std::cerr << "kv search actor duration: " << actorKvDuration.count() << "us" << std::endl;

                        auto [actorKvExtDuration, actorKvExtStat] =
                            BenchmarkKvActorExternalSender(threads, actors, iterations, dicts, keysToSearch);
                        std::cerr << "kv search actor with external message sender duration: "
                                << actorKvExtDuration.count() << "us" << std::endl;
                        Y_UNUSED(actorKvExtStat);


                        UNIT_ASSERT_EQUAL_C(actorKvStat, singleThreadKvStat,
                            "single thread found/not found: " << singleThreadKvStat.Found << "/" << singleThreadKvStat.NotFound << "; "
                            "actor stat found/not found: " << actorKvStat.Found << "/" << actorKvStat.NotFound);

                        std::cout << requestsNumber << ","
                                << dictsNumber << ","
                                << dictSize << ","
                                << threads << ","
                                << actors << ","
                                << actorKvDuration.count() << ","
                                << actorKvExtDuration.count() << ","
                                << tpKvDuration.count() << ","
                                << singleThreadKvDuration.count() << std::endl;
                    }
                    std::cerr << "----" << std::endl;
                }
            }
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
                    Y_ABORT_UNLESS(false);
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
                    Y_ABORT_UNLESS(false);
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
