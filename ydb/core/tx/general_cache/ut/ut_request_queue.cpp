#include <ydb/core/protos/config.pb.h>
#include <ydb/core/tx/general_cache/service/manager.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/digest/numeric.h>

#include <algorithm>
#include <util/generic/hash.h>
#include <util/generic/string.h>

namespace NGeneralCacheUt {

struct TAddress {
    ui64 Source = 0;
    ui64 Id = 0;
    ui64 Owner = 1;

    bool operator==(const TAddress& other) const {
        return Source == other.Source && Id == other.Id && Owner == other.Owner;
    }
};

}   // namespace NGeneralCacheUt

template <>
struct THash<NGeneralCacheUt::TAddress> {
    size_t operator()(const NGeneralCacheUt::TAddress& address) const {
        return CombineHashes(CombineHashes(THash<ui64>()(address.Source), THash<ui64>()(address.Id)), THash<ui64>()(address.Owner));
    }
};

namespace NGeneralCacheUt {

enum class EConsumer {
    FETCH_ON_LOAD,
    COMPACTION,
    SCAN,
};

inline NKikimr::NGeneralCache::NPublic::TCacheRequestClass GetCacheRequestClass(
    const EConsumer consumer, const NKikimr::NGeneralCache::NPublic::TConfig& config) {
    TString name;
    switch (consumer) {
        case EConsumer::FETCH_ON_LOAD:
            name = "FETCH_ON_LOAD";
            break;
        case EConsumer::COMPACTION:
            name = "COMPACTION";
            break;
        case EConsumer::SCAN:
            name = "SCAN";
            break;
    }
    return config.Classify(name);
}

template <bool Evict>
class TProcessor;

template <bool Evict>
class TPolicy {
public:
    using TAddress = NGeneralCacheUt::TAddress;
    using TObject = TString;
    using TSourceId = ui64;
    using EConsumer = NGeneralCacheUt::EConsumer;

    static bool EvictCacheWhenSourceGone(const TAddress& /*address*/) {
        return Evict;
    }

    static TSourceId GetSourceId(const TAddress& address) {
        return address.Source;
    }

    static NActors::TActorId GetRequestOwner(const TAddress& address) {
        return NActors::TActorId(address.Owner, 1);
    }

    static bool IsCacheEntryActual(const TAddress& /*address*/, const TObject& /*object*/) {
        return true;
    }

    static TString GetCacheName() {
        return "ut";
    }

    class TSizeCalcer {
    public:
        size_t operator()(const TObject&) const {
            return 1;
        }
    };

    static std::shared_ptr<NKikimr::NGeneralCache::NSource::IObjectsProcessor<TPolicy>> BuildObjectsProcessor(const NActors::TActorId&);
};

template <bool Evict>
struct TAsk {
    THashMap<EConsumer, THashSet<TAddress>> Addresses;

    NActors::TActorId Owner() const {
        UNIT_ASSERT(!Addresses.empty());
        return TPolicy<Evict>::GetRequestOwner(*Addresses.begin()->second.begin());
    }

    THashSet<ui64> Ids() const {
        THashSet<ui64> ids;
        for (const auto& [consumer, addresses] : Addresses) {
            Y_UNUSED(consumer);
            for (const auto& address : addresses) {
                ids.insert(address.Id);
            }
        }
        return ids;
    }
};

template <bool Evict>
class TProcessor: public NKikimr::NGeneralCache::NSource::IObjectsProcessor<TPolicy<Evict>> {
private:
    using TSelf = NKikimr::NGeneralCache::NSource::IObjectsProcessor<TPolicy<Evict>>;

    void DoAskData(const THashMap<EConsumer, THashSet<TAddress>>& objectAddressesByConsumer, const std::shared_ptr<TSelf>&) const override {
        if (objectAddressesByConsumer.empty()) {
            return;
        }
        Asks.push_back(TAsk<Evict>{ objectAddressesByConsumer });
    }

    void DoOnReceiveData(const ui64, const NActors::TActorId&, THashMap<TAddress, TString>&&, THashSet<TAddress>&&,
        THashMap<TAddress, TString>&&) const override {
    }

public:
    mutable std::vector<TAsk<Evict>> Asks;
};

template <bool Evict>
thread_local std::shared_ptr<TProcessor<Evict>> CurrentProcessor;

template <bool Evict>
std::shared_ptr<NKikimr::NGeneralCache::NSource::IObjectsProcessor<TPolicy<Evict>>> TPolicy<Evict>::BuildObjectsProcessor(
    const NActors::TActorId&) {
    CurrentProcessor<Evict> = std::make_shared<TProcessor<Evict>>();
    return CurrentProcessor<Evict>;
}

template <bool Evict>
class TCallback: public NKikimr::NGeneralCache::NPublic::ICallback<TPolicy<Evict>> {
private:
    void DoOnResultReady(THashMap<TAddress, TString>&&, THashSet<TAddress>&&,
        NKikimr::NGeneralCache::NPublic::TErrorAddresses<TPolicy<Evict>>&& errors) override {
        Ready = true;
        HasErrors = errors.HasErrors();
    }

    bool DoIsAborted() const override {
        return Aborted;
    }

public:
    bool Ready = false;
    bool HasErrors = false;
    bool Aborted = false;
};

template <bool Evict>
class THarness {
private:
    using TManager = NKikimr::NGeneralCache::NPrivate::TManager<TPolicy<Evict>>;
    using TRequest = NKikimr::NGeneralCache::NPrivate::TRequest<TPolicy<Evict>>;

    std::unique_ptr<NKikimr::NGeneralCache::NPrivate::TActorCounters> Counters;

public:
    std::unique_ptr<TManager> Manager;
    std::shared_ptr<TProcessor<Evict>> Processor;

    struct TQuota {
        const char* Name = "";
        ui32 Weight = 0;
    };

    explicit THarness(const ui64 sourceLimit, const std::initializer_list<TQuota> quotas = {}) {
        NKikimrConfig::TGeneralCacheConfig proto;
        proto.SetDirectInflightSourceLimit(sourceLimit);
        proto.SetDirectInflightGlobalLimit(1000);
        proto.SetMemoryLimit(1 << 20);
        for (const auto& quota : quotas) {
            auto* consumer = proto.AddConsumers();
            consumer->SetName(quota.Name);
            consumer->SetWeight(quota.Weight);
        }
        auto config = NKikimr::NGeneralCache::NPublic::TConfig::BuildFromProto(proto);
        UNIT_ASSERT_C(config.IsSuccess(), config.GetErrorMessage());
        auto root = MakeIntrusive<NMonitoring::TDynamicCounters>();
        Counters = std::make_unique<NKikimr::NGeneralCache::NPrivate::TActorCounters>("ut", root, config.GetResult());
        Manager = std::make_unique<TManager>(NActors::TActorId(), Counters->GetManager());
        Processor = CurrentProcessor<Evict>;
    }

    std::shared_ptr<TCallback<Evict>> Add(const ui64 source, const EConsumer consumer, const std::vector<ui64>& ids, const ui64 owner = 1,
        const bool aborted = false) {
        THashSet<TAddress> addresses;
        for (const ui64 id : ids) {
            addresses.insert(TAddress{ source, id, owner });
        }
        auto callback = std::make_shared<TCallback<Evict>>();
        callback->Aborted = aborted;
        std::shared_ptr<NKikimr::NGeneralCache::NPublic::ICallback<TPolicy<Evict>>> base = callback;
        Manager->AddRequest(std::make_shared<TRequest>(std::move(addresses), std::move(base), consumer, TMonotonic::Now()));
        return callback;
    }

    void Finish(TAsk<Evict> ask) {
        THashMap<TAddress, TString> objects;
        ui64 source = 0;
        for (const auto& [consumer, addresses] : ask.Addresses) {
            Y_UNUSED(consumer);
            for (const auto& address : addresses) {
                source = address.Source;
                objects.emplace(address, "value");
            }
        }
        UNIT_ASSERT(!objects.empty());
        Manager->OnRequestResult(source, std::move(objects), {}, {}, ask.Owner());
    }
};

void AssertIds(const THashSet<ui64>& ids, const std::vector<ui64>& allowed, const size_t count) {
    UNIT_ASSERT_VALUES_EQUAL(ids.size(), count);
    for (const ui64 id : ids) {
        UNIT_ASSERT(std::find(allowed.begin(), allowed.end(), id) != allowed.end());
    }
}

}   // namespace NGeneralCacheUt

Y_UNIT_TEST_SUITE(TGeneralCacheRequestQueue) {
    Y_UNIT_TEST(WithoutWeightsConsumersShareOneFifo) {
        NGeneralCacheUt::THarness<false> harness(2);
        harness.Add(1, NGeneralCacheUt::EConsumer::FETCH_ON_LOAD, { 1, 2, 3, 4, 5 });
        auto scan = harness.Add(1, NGeneralCacheUt::EConsumer::SCAN, { 20 });
        UNIT_ASSERT_VALUES_EQUAL(harness.Processor->Asks.size(), 1);
        NGeneralCacheUt::AssertIds(harness.Processor->Asks[0].Ids(), { 1, 2, 3, 4, 5 }, 2);
        UNIT_ASSERT(!scan->Ready);
    }

    Y_UNIT_TEST(PrefetchIsChunkedBySourceLimit) {
        NGeneralCacheUt::THarness<false> harness(2);
        auto callback = harness.Add(1, NGeneralCacheUt::EConsumer::FETCH_ON_LOAD, { 1, 2, 3, 4, 5 });
        UNIT_ASSERT(!callback->Ready);
        UNIT_ASSERT_VALUES_EQUAL(harness.Processor->Asks.size(), 1);
        NGeneralCacheUt::AssertIds(harness.Processor->Asks[0].Ids(), { 1, 2, 3, 4, 5 }, 2);

        harness.Finish(harness.Processor->Asks[0]);
        UNIT_ASSERT(!callback->Ready);
        UNIT_ASSERT_VALUES_EQUAL(harness.Processor->Asks.size(), 2);
        NGeneralCacheUt::AssertIds(harness.Processor->Asks[1].Ids(), { 1, 2, 3, 4, 5 }, 2);
        for (const ui64 id : harness.Processor->Asks[1].Ids()) {
            UNIT_ASSERT(!harness.Processor->Asks[0].Ids().contains(id));
        }

        harness.Finish(harness.Processor->Asks[1]);
        UNIT_ASSERT_VALUES_EQUAL(harness.Processor->Asks.size(), 3);
        NGeneralCacheUt::AssertIds(harness.Processor->Asks.at(2).Ids(), { 1, 2, 3, 4, 5 }, 1);
        harness.Finish(harness.Processor->Asks[2]);
        UNIT_ASSERT(callback->Ready);
        UNIT_ASSERT(!callback->HasErrors);
    }

    Y_UNIT_TEST(ScanAndCompactionGoAheadOfPrefetchTail) {
        NGeneralCacheUt::THarness<false> harness(2, { { "SCAN", 1 }, { "COMPACTION", 1 } });
        auto prefetch = harness.Add(1, NGeneralCacheUt::EConsumer::FETCH_ON_LOAD, { 1, 2, 3, 4, 5 });
        const auto prefetchAsk = harness.Processor->Asks.at(0);
        NGeneralCacheUt::AssertIds(prefetchAsk.Ids(), { 1, 2, 3, 4, 5 }, 2);

        auto compaction = harness.Add(1, NGeneralCacheUt::EConsumer::COMPACTION, { 10 });
        auto scan = harness.Add(1, NGeneralCacheUt::EConsumer::SCAN, { 20 });
        UNIT_ASSERT_VALUES_EQUAL(harness.Processor->Asks.size(), 1);
        UNIT_ASSERT(!scan->Ready);
        UNIT_ASSERT(!compaction->Ready);

        harness.Finish(prefetchAsk);
        UNIT_ASSERT_VALUES_EQUAL(harness.Processor->Asks.size(), 2);
        NGeneralCacheUt::AssertIds(harness.Processor->Asks[1].Ids(), { 10, 20 }, 2);
        UNIT_ASSERT(!prefetch->Ready);

        harness.Finish(harness.Processor->Asks[1]);
        UNIT_ASSERT(scan->Ready);
        UNIT_ASSERT(compaction->Ready);
        UNIT_ASSERT(!prefetch->Ready);
    }

    Y_UNIT_TEST(ScansStayFifoInsideOneQueue) {
        NGeneralCacheUt::THarness<false> harness(1);
        auto first = harness.Add(1, NGeneralCacheUt::EConsumer::SCAN, { 1, 2 });
        const auto firstChunk = harness.Processor->Asks.at(0);
        NGeneralCacheUt::AssertIds(firstChunk.Ids(), { 1, 2 }, 1);

        auto second = harness.Add(1, NGeneralCacheUt::EConsumer::SCAN, { 3 });
        UNIT_ASSERT_VALUES_EQUAL(harness.Processor->Asks.size(), 1);
        UNIT_ASSERT(!second->Ready);

        harness.Finish(firstChunk);
        const auto firstTail = harness.Processor->Asks.at(1);
        NGeneralCacheUt::AssertIds(firstTail.Ids(), { 1, 2 }, 1);
        UNIT_ASSERT(!firstChunk.Ids().contains(*firstTail.Ids().begin()));
        UNIT_ASSERT(!second->Ready);

        harness.Finish(firstTail);
        NGeneralCacheUt::AssertIds(harness.Processor->Asks.at(2).Ids(), { 3 }, 1);
        harness.Finish(harness.Processor->Asks.at(2));
        UNIT_ASSERT(first->Ready);
        UNIT_ASSERT(second->Ready);
    }

    Y_UNIT_TEST(WeightsShareTheSourceLimit) {
        NGeneralCacheUt::THarness<false> harness(4, { { "SCAN", 3 }, { "COMPACTION", 1 } });
        auto scan = harness.Add(1, NGeneralCacheUt::EConsumer::SCAN, { 1, 2, 3 });
        NGeneralCacheUt::AssertIds(harness.Processor->Asks.at(0).Ids(), { 1, 2, 3 }, 3);

        auto compaction = harness.Add(1, NGeneralCacheUt::EConsumer::COMPACTION, { 10, 11 });
        UNIT_ASSERT_VALUES_EQUAL(harness.Processor->Asks.size(), 2);
        NGeneralCacheUt::AssertIds(harness.Processor->Asks.at(1).Ids(), { 10, 11 }, 1);

        auto extra = harness.Add(1, NGeneralCacheUt::EConsumer::SCAN, { 4 });
        UNIT_ASSERT_VALUES_EQUAL(harness.Processor->Asks.size(), 2);
        UNIT_ASSERT(!extra->Ready);

        harness.Finish(harness.Processor->Asks.at(1));
        UNIT_ASSERT_VALUES_EQUAL(harness.Processor->Asks.size(), 3);
        NGeneralCacheUt::AssertIds(harness.Processor->Asks.at(2).Ids(), { 10, 11 }, 1);
        UNIT_ASSERT(!harness.Processor->Asks.at(1).Ids().contains(*harness.Processor->Asks.at(2).Ids().begin()));
        UNIT_ASSERT(!scan->Ready);
        UNIT_ASSERT(!extra->Ready);
    }

    Y_UNIT_TEST(LargestRemainderGetsTheSpareSlot) {
        NGeneralCacheUt::THarness<false> harness(5, { { "SCAN", 2 }, { "COMPACTION", 1 } });
        harness.Add(1, NGeneralCacheUt::EConsumer::FETCH_ON_LOAD, { 100, 101, 102, 103, 104 });
        harness.Add(1, NGeneralCacheUt::EConsumer::SCAN, { 1, 2, 3, 4, 5, 6 });
        harness.Add(1, NGeneralCacheUt::EConsumer::COMPACTION, { 11, 12, 13, 14, 15, 16 });
        UNIT_ASSERT_VALUES_EQUAL(harness.Processor->Asks.size(), 1);

        // 5 * 2 / 3 = 3 rest 1, 5 * 1 / 3 = 1 rest 2: the spare slot goes to compaction.
        harness.Finish(harness.Processor->Asks.at(0));
        UNIT_ASSERT_VALUES_EQUAL(harness.Processor->Asks.size(), 2);
        const auto ids = harness.Processor->Asks.at(1).Ids();
        const auto scanCount = std::count_if(ids.begin(), ids.end(), [](const ui64 id) {
            return id < 10;
        });
        UNIT_ASSERT_VALUES_EQUAL(ids.size(), 5);
        UNIT_ASSERT_VALUES_EQUAL(scanCount, 3);
    }

    Y_UNIT_TEST(SmallWeightStillGetsOneSlot) {
        NGeneralCacheUt::THarness<false> harness(2, { { "SCAN", 3 }, { "COMPACTION", 1 } });
        harness.Add(1, NGeneralCacheUt::EConsumer::FETCH_ON_LOAD, { 100, 101 });
        auto scan = harness.Add(1, NGeneralCacheUt::EConsumer::SCAN, { 1, 2, 3, 4, 5 });
        auto compaction = harness.Add(1, NGeneralCacheUt::EConsumer::COMPACTION, { 10 });
        UNIT_ASSERT_VALUES_EQUAL(harness.Processor->Asks.size(), 1);

        // 2 * 1 / 4 rounds to 0: without a minimum share compaction waits for the whole scan.
        harness.Finish(harness.Processor->Asks.at(0));
        UNIT_ASSERT_VALUES_EQUAL(harness.Processor->Asks.size(), 2);
        const auto ids = harness.Processor->Asks.at(1).Ids();
        UNIT_ASSERT_VALUES_EQUAL(ids.size(), 2);
        UNIT_ASSERT(ids.contains(10));

        harness.Finish(harness.Processor->Asks.at(1));
        UNIT_ASSERT(compaction->Ready);
        UNIT_ASSERT(!scan->Ready);
    }

    Y_UNIT_TEST(ZeroWeightWaitsWhileWeightedRequestsAreQueued) {
        NGeneralCacheUt::THarness<false> harness(2, { { "SCAN", 1 } });
        auto scan = harness.Add(1, NGeneralCacheUt::EConsumer::SCAN, { 1, 2, 3, 4, 5 });
        auto prefetch = harness.Add(1, NGeneralCacheUt::EConsumer::FETCH_ON_LOAD, { 10 });
        NGeneralCacheUt::AssertIds(harness.Processor->Asks.at(0).Ids(), { 1, 2, 3, 4, 5 }, 2);

        harness.Finish(harness.Processor->Asks.at(0));
        UNIT_ASSERT_VALUES_EQUAL(harness.Processor->Asks.size(), 2);
        NGeneralCacheUt::AssertIds(harness.Processor->Asks.at(1).Ids(), { 1, 2, 3, 4, 5 }, 2);
        UNIT_ASSERT(!prefetch->Ready);

        harness.Finish(harness.Processor->Asks.at(1));
        UNIT_ASSERT_VALUES_EQUAL(harness.Processor->Asks.size(), 3);
        const auto tail = harness.Processor->Asks.at(2).Ids();
        UNIT_ASSERT_VALUES_EQUAL(tail.size(), 2);
        UNIT_ASSERT(tail.contains(10));

        harness.Finish(harness.Processor->Asks.at(2));
        UNIT_ASSERT(scan->Ready);
        UNIT_ASSERT(prefetch->Ready);
    }

    Y_UNIT_TEST(AbortedWeightedRequestDoesNotStallZeroWeight) {
        NGeneralCacheUt::THarness<false> harness(1, { { "SCAN", 1 } });
        auto scan = harness.Add(1, NGeneralCacheUt::EConsumer::SCAN, { 1 });
        NGeneralCacheUt::AssertIds(harness.Processor->Asks.at(0).Ids(), { 1 }, 1);

        auto aborted = harness.Add(1, NGeneralCacheUt::EConsumer::SCAN, { 2 });
        aborted->Aborted = true;
        auto compaction = harness.Add(1, NGeneralCacheUt::EConsumer::COMPACTION, { 10 });
        UNIT_ASSERT_VALUES_EQUAL(harness.Processor->Asks.size(), 1);
        UNIT_ASSERT(!aborted->Ready);
        UNIT_ASSERT(!compaction->Ready);

        harness.Finish(harness.Processor->Asks.at(0));
        UNIT_ASSERT(scan->Ready);
        UNIT_ASSERT_VALUES_EQUAL(harness.Processor->Asks.size(), 2);
        NGeneralCacheUt::AssertIds(harness.Processor->Asks.at(1).Ids(), { 10 }, 1);
        UNIT_ASSERT(!aborted->Ready);
    }

    Y_UNIT_TEST(ResultFromOldOwnerDoesNotTouchTheNewSource) {
        NGeneralCacheUt::THarness<false> harness(2);
        auto first = harness.Add(1, NGeneralCacheUt::EConsumer::FETCH_ON_LOAD, { 1 }, 1);
        const auto oldAsk = harness.Processor->Asks.at(0);
        const NGeneralCacheUt::TAddress oldAddress = *oldAsk.Addresses.begin()->second.begin();
        harness.Manager->AbortSource(1);
        UNIT_ASSERT(first->Ready);
        UNIT_ASSERT(first->HasErrors);

        THashMap<NGeneralCacheUt::TAddress, TString> whileAbsent;
        whileAbsent.emplace(oldAddress, "stale");
        harness.Manager->OnRequestResult(1, std::move(whileAbsent), {}, {}, oldAsk.Owner());

        auto second = harness.Add(1, NGeneralCacheUt::EConsumer::FETCH_ON_LOAD, { 1 }, 2);
        const auto freshAsk = harness.Processor->Asks.back();
        UNIT_ASSERT(freshAsk.Owner() != oldAsk.Owner());
        const NGeneralCacheUt::TAddress address = *freshAsk.Addresses.begin()->second.begin();
        UNIT_ASSERT(!second->Ready);

        THashMap<NGeneralCacheUt::TAddress, TString> errors;
        errors.emplace(address, "relocation of the tablet");
        harness.Manager->OnRequestResult(1, {}, {}, std::move(errors), oldAsk.Owner());
        UNIT_ASSERT(!second->Ready);
        UNIT_ASSERT(!second->HasErrors);

        THashMap<NGeneralCacheUt::TAddress, TString> cached;
        cached.emplace(address, "cached");
        harness.Manager->OnRequestResult(1, std::move(cached), {}, {}, oldAsk.Owner());
        UNIT_ASSERT(!second->Ready);

        auto third = harness.Add(1, NGeneralCacheUt::EConsumer::SCAN, { 1 }, 2);
        UNIT_ASSERT(!third->Ready);

        THashMap<NGeneralCacheUt::TAddress, TString> real;
        real.emplace(address, "real");
        harness.Manager->OnRequestResult(1, std::move(real), {}, {}, freshAsk.Owner());
        UNIT_ASSERT(second->Ready);
        UNIT_ASSERT(third->Ready);
        UNIT_ASSERT(!third->HasErrors);
    }

    Y_UNIT_TEST(ResultForAbsentSourceDoesNotChangeCache) {
        NGeneralCacheUt::THarness<false> harness(2);
        auto first = harness.Add(1, NGeneralCacheUt::EConsumer::FETCH_ON_LOAD, { 1 });
        const auto ask = harness.Processor->Asks.at(0);
        const NGeneralCacheUt::TAddress address = *ask.Addresses.begin()->second.begin();
        harness.Finish(ask);
        UNIT_ASSERT(first->Ready);
        harness.Manager->AbortSource(1);

        THashSet<NGeneralCacheUt::TAddress> removed;
        removed.insert(address);
        harness.Manager->OnRequestResult(1, {}, std::move(removed), {}, ask.Owner());

        const size_t asks = harness.Processor->Asks.size();
        auto again = harness.Add(1, NGeneralCacheUt::EConsumer::FETCH_ON_LOAD, { 1 });
        UNIT_ASSERT(again->Ready);
        UNIT_ASSERT_VALUES_EQUAL(harness.Processor->Asks.size(), asks);
    }

    Y_UNIT_TEST(DeadSourceEntriesStayWhenEvictionIsOff) {
        NGeneralCacheUt::THarness<false> harness(2);
        auto first = harness.Add(1, NGeneralCacheUt::EConsumer::FETCH_ON_LOAD, { 1 });
        harness.Finish(harness.Processor->Asks.back());
        UNIT_ASSERT(first->Ready);
        harness.Manager->AbortSource(1);

        auto second = harness.Add(2, NGeneralCacheUt::EConsumer::FETCH_ON_LOAD, { 2 });
        harness.Finish(harness.Processor->Asks.back());
        UNIT_ASSERT(second->Ready);

        const size_t asks = harness.Processor->Asks.size();
        auto again = harness.Add(1, NGeneralCacheUt::EConsumer::FETCH_ON_LOAD, { 1 });
        UNIT_ASSERT(again->Ready);
        UNIT_ASSERT_VALUES_EQUAL(harness.Processor->Asks.size(), asks);
    }

    Y_UNIT_TEST(DeadSourceEntriesAreDroppedWhenEvictionIsOn) {
        NGeneralCacheUt::THarness<true> harness(2);
        auto first = harness.Add(1, NGeneralCacheUt::EConsumer::FETCH_ON_LOAD, { 1 });
        harness.Finish(harness.Processor->Asks.back());
        UNIT_ASSERT(first->Ready);
        harness.Manager->AbortSource(1);

        auto second = harness.Add(2, NGeneralCacheUt::EConsumer::FETCH_ON_LOAD, { 2 });
        harness.Finish(harness.Processor->Asks.back());
        UNIT_ASSERT(second->Ready);

        const size_t asks = harness.Processor->Asks.size();
        auto again = harness.Add(1, NGeneralCacheUt::EConsumer::FETCH_ON_LOAD, { 1 });
        UNIT_ASSERT(!again->Ready);
        UNIT_ASSERT_VALUES_EQUAL(harness.Processor->Asks.size(), asks + 1);
        harness.Finish(harness.Processor->Asks.back());
        UNIT_ASSERT(again->Ready);
    }

    Y_UNIT_TEST(PushedEntriesOfLiveSourceStayWhenEvictionIsOn) {
        NGeneralCacheUt::THarness<true> harness(2);
        THashMap<NGeneralCacheUt::TAddress, TString> pushed;
        pushed.emplace(NGeneralCacheUt::TAddress{ 1, 1, 1 }, "pushed");
        harness.Manager->OnAdditionalObjectsInfo(1, std::move(pushed), {});

        THashMap<NGeneralCacheUt::TAddress, TString> other;
        other.emplace(NGeneralCacheUt::TAddress{ 2, 2, 2 }, "other");
        harness.Manager->OnAdditionalObjectsInfo(2, std::move(other), {});

        const size_t asks = harness.Processor->Asks.size();
        auto read = harness.Add(1, NGeneralCacheUt::EConsumer::SCAN, { 1 });
        UNIT_ASSERT(read->Ready);
        UNIT_ASSERT_VALUES_EQUAL(harness.Processor->Asks.size(), asks);
    }

    Y_UNIT_TEST(StaleKillDoesNotAbortTheNewIncarnation) {
        NGeneralCacheUt::THarness<false> harness(2);
        auto previous = harness.Add(1, NGeneralCacheUt::EConsumer::FETCH_ON_LOAD, { 1 }, 1);
        UNIT_ASSERT(!previous->Ready);

        auto current = harness.Add(1, NGeneralCacheUt::EConsumer::SCAN, { 2 }, 2);
        UNIT_ASSERT(previous->Ready);
        UNIT_ASSERT(previous->HasErrors);
        UNIT_ASSERT(!current->Ready);

        harness.Manager->AbortSource(1, NActors::TActorId(1, 1));
        UNIT_ASSERT(!current->Ready);

        harness.Manager->AbortSource(1, NActors::TActorId(2, 1));
        UNIT_ASSERT(current->Ready);
        UNIT_ASSERT(current->HasErrors);
    }

    Y_UNIT_TEST(UndeliveredAbortsOnlyTheSourceOfThatActor) {
        NGeneralCacheUt::THarness<false> harness(2);
        auto current = harness.Add(1, NGeneralCacheUt::EConsumer::SCAN, { 1 }, 2);
        harness.Manager->AbortSourceOfOwner(NActors::TActorId(1, 1));
        UNIT_ASSERT(!current->Ready);

        harness.Manager->AbortSourceOfOwner(NActors::TActorId(2, 1));
        UNIT_ASSERT(current->Ready);
        UNIT_ASSERT(current->HasErrors);
    }

    Y_UNIT_TEST(StalePortionUpdateDoesNotTouchTheNewIncarnation) {
        NGeneralCacheUt::THarness<false> harness(2);
        auto previous = harness.Add(1, NGeneralCacheUt::EConsumer::FETCH_ON_LOAD, { 1 }, 1);
        auto current = harness.Add(1, NGeneralCacheUt::EConsumer::SCAN, { 2 }, 2);
        UNIT_ASSERT(previous->Ready);
        UNIT_ASSERT(!current->Ready);

        THashSet<NGeneralCacheUt::TAddress> staleRemove;
        staleRemove.insert(NGeneralCacheUt::TAddress{ 1, 2, 1 });
        harness.Manager->OnAdditionalObjectsInfo(1, {}, std::move(staleRemove));
        UNIT_ASSERT(!current->Ready);

        THashSet<NGeneralCacheUt::TAddress> remove;
        remove.insert(NGeneralCacheUt::TAddress{ 1, 2, 2 });
        harness.Manager->OnAdditionalObjectsInfo(1, {}, std::move(remove));
        UNIT_ASSERT(current->Ready);
        UNIT_ASSERT(!current->HasErrors);
    }
}
