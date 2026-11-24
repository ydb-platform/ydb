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

    bool operator==(const TAddress& other) const {
        return Source == other.Source && Id == other.Id;
    }
};

}   // namespace NGeneralCacheUt

template <>
struct THash<NGeneralCacheUt::TAddress> {
    size_t operator()(const NGeneralCacheUt::TAddress& address) const {
        return CombineHashes(THash<ui64>()(address.Source), THash<ui64>()(address.Id));
    }
};

namespace NGeneralCacheUt {

enum class EConsumer {
    FETCH_ON_LOAD,
    COMPACTION,
    SCAN,
};

}   // namespace NGeneralCacheUt

template <>
void Out<NGeneralCacheUt::EConsumer>(IOutputStream& out, const NGeneralCacheUt::EConsumer consumer) {
    switch (consumer) {
        case NGeneralCacheUt::EConsumer::FETCH_ON_LOAD:
            out << "FETCH_ON_LOAD";
            break;
        case NGeneralCacheUt::EConsumer::COMPACTION:
            out << "COMPACTION";
            break;
        case NGeneralCacheUt::EConsumer::SCAN:
            out << "SCAN";
            break;
    }
}

namespace NGeneralCacheUt {

class TProcessor;

class TPolicy {
public:
    using TAddress = NGeneralCacheUt::TAddress;
    using TObject = TString;
    using TSourceId = ui64;
    using EConsumer = NGeneralCacheUt::EConsumer;

    static TSourceId GetSourceId(const TAddress& address) {
        return address.Source;
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

struct TAsk {
    THashMap<EConsumer, THashSet<TAddress>> Addresses;

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

class TProcessor: public NKikimr::NGeneralCache::NSource::IObjectsProcessor<TPolicy> {
private:
    using TSelf = NKikimr::NGeneralCache::NSource::IObjectsProcessor<TPolicy>;

    void DoAskData(const THashMap<NGeneralCacheUt::EConsumer, THashSet<NGeneralCacheUt::TAddress>>& objectAddressesByConsumer,
        const std::shared_ptr<TSelf>&, const ui64) const override {
        if (objectAddressesByConsumer.empty()) {
            return;
        }
        Asks.push_back(TAsk{ objectAddressesByConsumer });
    }

    void DoOnReceiveData(const ui64, THashMap<NGeneralCacheUt::TAddress, TString>&&, THashSet<NGeneralCacheUt::TAddress>&&,
        THashMap<NGeneralCacheUt::TAddress, TString>&&) const override {
    }

public:
    mutable std::vector<TAsk> Asks;
};

thread_local std::shared_ptr<TProcessor> CurrentProcessor;

std::shared_ptr<NKikimr::NGeneralCache::NSource::IObjectsProcessor<TPolicy>> TPolicy::BuildObjectsProcessor(const NActors::TActorId&) {
    CurrentProcessor = std::make_shared<TProcessor>();
    return CurrentProcessor;
}

class TCallback: public NKikimr::NGeneralCache::NPublic::ICallback<TPolicy> {
private:
    void DoOnResultReady(THashMap<NGeneralCacheUt::TAddress, TString>&&, THashSet<NGeneralCacheUt::TAddress>&&,
        NKikimr::NGeneralCache::NPublic::TErrorAddresses<TPolicy>&& errors) override {
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

class THarness {
private:
    using TManager = NKikimr::NGeneralCache::NPrivate::TManager<TPolicy>;
    using TRequest = NKikimr::NGeneralCache::NPrivate::TRequest<TPolicy>;

    std::unique_ptr<NKikimr::NGeneralCache::NPrivate::TActorCounters> Counters;

public:
    std::unique_ptr<TManager> Manager;
    std::shared_ptr<TProcessor> Processor;

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
        Processor = CurrentProcessor;
    }

    std::shared_ptr<TCallback> Add(const ui64 source, const EConsumer consumer, const std::vector<ui64>& ids) {
        std::vector<TAddress> addresses;
        for (const ui64 id : ids) {
            addresses.push_back(TAddress{ source, id });
        }
        return Add(consumer, addresses);
    }

    std::shared_ptr<TCallback> Add(const EConsumer consumer, const std::vector<TAddress>& list) {
        THashSet<TAddress> addresses(list.begin(), list.end());
        auto callback = std::make_shared<TCallback>();
        std::shared_ptr<NKikimr::NGeneralCache::NPublic::ICallback<TPolicy>> base = callback;
        Manager->AddRequest(std::make_shared<TRequest>(std::move(addresses), std::move(base), consumer, TMonotonic::Now()));
        return callback;
    }

    void Finish(TAsk ask) {
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
        Manager->OnRequestResult(source, std::move(objects), {}, {});
    }

    void Finish(const ui64 source, const std::vector<ui64>& ids) {
        TAsk ask;
        for (const ui64 id : ids) {
            ask.Addresses[EConsumer::SCAN].insert(TAddress{ source, id });
        }
        Finish(std::move(ask));
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
    Y_UNIT_TEST(WithoutWeightsRequestsAreSentWholeInOneFifo) {
        NGeneralCacheUt::THarness harness(2);
        auto prefetch = harness.Add(1, NGeneralCacheUt::EConsumer::FETCH_ON_LOAD, { 1, 2, 3, 4, 5 });
        auto scan = harness.Add(1, NGeneralCacheUt::EConsumer::SCAN, { 20 });
        UNIT_ASSERT_VALUES_EQUAL(harness.Processor->Asks.size(), 1);
        NGeneralCacheUt::AssertIds(harness.Processor->Asks[0].Ids(), { 1, 2, 3, 4, 5 }, 5);
        UNIT_ASSERT(!scan->Ready);

        harness.Finish(harness.Processor->Asks[0]);
        UNIT_ASSERT(prefetch->Ready);
        UNIT_ASSERT_VALUES_EQUAL(harness.Processor->Asks.size(), 2);
        NGeneralCacheUt::AssertIds(harness.Processor->Asks[1].Ids(), { 20 }, 1);
    }

    Y_UNIT_TEST(PrefetchIsChunkedBySourceLimit) {
        NGeneralCacheUt::THarness harness(2, { { "SCAN", 1 } });
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
        NGeneralCacheUt::THarness harness(2, { { "SCAN", 1 }, { "COMPACTION", 1 } });
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
        NGeneralCacheUt::THarness harness(1, { { "SCAN", 1 } });
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
        NGeneralCacheUt::THarness harness(4, { { "SCAN", 3 }, { "COMPACTION", 1 } });
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

    Y_UNIT_TEST(BacklogIsSplitByWeights) {
        NGeneralCacheUt::THarness harness(4, { { "SCAN", 3 }, { "COMPACTION", 1 } });
        harness.Add(1, NGeneralCacheUt::EConsumer::FETCH_ON_LOAD, { 100, 101, 102, 103 });
        harness.Add(1, NGeneralCacheUt::EConsumer::SCAN, { 1, 2, 3, 4, 5, 6 });
        harness.Add(1, NGeneralCacheUt::EConsumer::COMPACTION, { 11, 12, 13, 14, 15, 16 });
        UNIT_ASSERT_VALUES_EQUAL(harness.Processor->Asks.size(), 1);

        harness.Finish(harness.Processor->Asks.at(0));
        UNIT_ASSERT_VALUES_EQUAL(harness.Processor->Asks.size(), 2);
        const auto ids = harness.Processor->Asks.at(1).Ids();
        const auto scanCount = std::count_if(ids.begin(), ids.end(), [](const ui64 id) {
            return id < 10;
        });
        UNIT_ASSERT_VALUES_EQUAL(ids.size(), 4);
        UNIT_ASSERT_VALUES_EQUAL(scanCount, 3);
    }

    Y_UNIT_TEST(FewerSlotsThanQueuesDoNotStarveAWeight) {
        NGeneralCacheUt::THarness harness(1, { { "SCAN", 8 }, { "COMPACTION", 2 } });
        auto scan = harness.Add(1, NGeneralCacheUt::EConsumer::SCAN, { 1, 2, 3, 4, 5 });
        auto compaction = harness.Add(1, NGeneralCacheUt::EConsumer::COMPACTION, { 10 });
        NGeneralCacheUt::AssertIds(harness.Processor->Asks.at(0).Ids(), { 1, 2, 3, 4, 5 }, 1);

        harness.Finish(harness.Processor->Asks.at(0));
        UNIT_ASSERT_VALUES_EQUAL(harness.Processor->Asks.size(), 2);
        NGeneralCacheUt::AssertIds(harness.Processor->Asks.at(1).Ids(), { 10 }, 1);
        harness.Finish(harness.Processor->Asks.at(1));
        UNIT_ASSERT(compaction->Ready);
        UNIT_ASSERT(!scan->Ready);
    }

    Y_UNIT_TEST(SmallWeightStillGetsOneSlot) {
        NGeneralCacheUt::THarness harness(2, { { "SCAN", 3 }, { "COMPACTION", 1 } });
        harness.Add(1, NGeneralCacheUt::EConsumer::FETCH_ON_LOAD, { 100, 101 });
        auto scan = harness.Add(1, NGeneralCacheUt::EConsumer::SCAN, { 1, 2, 3, 4, 5 });
        auto compaction = harness.Add(1, NGeneralCacheUt::EConsumer::COMPACTION, { 10 });
        UNIT_ASSERT_VALUES_EQUAL(harness.Processor->Asks.size(), 1);

        // 2 * 1 / 4 rounds to 0: without a minimum of one slot compaction waits for the whole scan.
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
        NGeneralCacheUt::THarness harness(2, { { "SCAN", 1 } });
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
        NGeneralCacheUt::THarness harness(1, { { "SCAN", 1 } });
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

    Y_UNIT_TEST(RequestAbortedAfterFirstChunkNeverFetchesItsTail) {
        NGeneralCacheUt::THarness harness(2, { { "SCAN", 1 } });
        auto scan = harness.Add(1, NGeneralCacheUt::EConsumer::SCAN, { 1, 2, 3, 4, 5 });
        const auto sent = harness.Processor->Asks.at(0).Ids();
        NGeneralCacheUt::AssertIds(sent, { 1, 2, 3, 4, 5 }, 2);
        std::vector<ui64> sentIds(sent.begin(), sent.end());
        scan->Aborted = true;

        harness.Finish(1, { sentIds[0] });
        UNIT_ASSERT_VALUES_EQUAL(harness.Processor->Asks.size(), 1);
        UNIT_ASSERT(!scan->Ready);

        harness.Finish(1, { sentIds[1] });
        UNIT_ASSERT_VALUES_EQUAL(harness.Processor->Asks.size(), 1);
        UNIT_ASSERT(scan->Ready);
        UNIT_ASSERT(scan->HasErrors);

        auto next = harness.Add(1, NGeneralCacheUt::EConsumer::SCAN, { 10, 11 });
        UNIT_ASSERT_VALUES_EQUAL(harness.Processor->Asks.size(), 2);
        NGeneralCacheUt::AssertIds(harness.Processor->Asks.at(1).Ids(), { 10, 11 }, 2);
        harness.Finish(harness.Processor->Asks.at(1));
        UNIT_ASSERT(next->Ready);
        UNIT_ASSERT(!next->HasErrors);
    }

    Y_UNIT_TEST(BrokenSourceFinishesPartlySentAndQueuedRequests) {
        NGeneralCacheUt::THarness harness(2, { { "SCAN", 1 } });
        auto scan = harness.Add(1, NGeneralCacheUt::EConsumer::SCAN, { 1, 2, 3, 4, 5 });
        auto prefetch = harness.Add(1, NGeneralCacheUt::EConsumer::FETCH_ON_LOAD, { 20 });
        const auto sentAsk = harness.Processor->Asks.at(0);
        NGeneralCacheUt::AssertIds(sentAsk.Ids(), { 1, 2, 3, 4, 5 }, 2);

        harness.Manager->AbortSource(1);
        UNIT_ASSERT(scan->Ready);
        UNIT_ASSERT(scan->HasErrors);
        UNIT_ASSERT(prefetch->Ready);
        UNIT_ASSERT(prefetch->HasErrors);

        harness.Finish(sentAsk);
        UNIT_ASSERT_VALUES_EQUAL(harness.Processor->Asks.size(), 1);

        auto next = harness.Add(1, NGeneralCacheUt::EConsumer::SCAN, { 10, 11 });
        UNIT_ASSERT_VALUES_EQUAL(harness.Processor->Asks.size(), 2);
        NGeneralCacheUt::AssertIds(harness.Processor->Asks.at(1).Ids(), { 10, 11 }, 2);
        harness.Finish(harness.Processor->Asks.at(1));
        UNIT_ASSERT(next->Ready);
        UNIT_ASSERT(!next->HasErrors);
    }

    Y_UNIT_TEST(RequestOnTwoSourcesWaitsForBoth) {
        NGeneralCacheUt::THarness harness(1, { { "SCAN", 1 } });
        auto scan = harness.Add(NGeneralCacheUt::EConsumer::SCAN, { { 1, 1 }, { 2, 2 } });
        UNIT_ASSERT_VALUES_EQUAL(harness.Processor->Asks.size(), 2);
        auto queued = harness.Add(2, NGeneralCacheUt::EConsumer::SCAN, { 3 });
        UNIT_ASSERT_VALUES_EQUAL(harness.Processor->Asks.size(), 2);

        harness.Finish(1, { 1 });
        UNIT_ASSERT(!scan->Ready);
        UNIT_ASSERT_VALUES_EQUAL(harness.Processor->Asks.size(), 2);

        harness.Finish(2, { 2 });
        UNIT_ASSERT(scan->Ready);
        UNIT_ASSERT(!scan->HasErrors);
        UNIT_ASSERT_VALUES_EQUAL(harness.Processor->Asks.size(), 3);
        NGeneralCacheUt::AssertIds(harness.Processor->Asks.at(2).Ids(), { 3 }, 1);
        harness.Finish(harness.Processor->Asks.at(2));
        UNIT_ASSERT(queued->Ready);
    }
}
