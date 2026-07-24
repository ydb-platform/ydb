#include <ydb/core/blobstorage/vdisk/hulldb/barriers/barriers_essence.h>
#include <ydb/core/blobstorage/vdisk/hulldb/barriers/barriers_tree.h>
#include <ydb/core/blobstorage/vdisk/hulldb/barriers/hullds_cache_barrier.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/hash.h>
#include <util/system/yassert.h>

#include <algorithm>
#include <tuple>

namespace {

using namespace NKikimr;

constexpr ui32 MaxOps = 256;
constexpr ui64 TabletBase = 900000;
constexpr ui32 NumTablets = 8;
constexpr ui32 NumChannels = 4;
const TString VDiskLogPrefix = "barriers-fuzz";

struct TBarrierPoint {
    ui32 CollectGen = 0;
    ui32 CollectStep = 0;
    bool Dead = false;

    auto AsTuple() const {
        return std::make_tuple(CollectGen, CollectStep);
    }
};

struct TOracleKey {
    ui64 TabletId = 0;
    ui32 Channel = 0;
    bool Hard = false;

    bool operator==(const TOracleKey& other) const {
        return TabletId == other.TabletId && Channel == other.Channel && Hard == other.Hard;
    }
};

} // namespace

template <>
struct THash<TOracleKey> {
    size_t operator()(const TOracleKey& key) const {
        return CombineHashes(CombineHashes(IntHash<size_t>(key.TabletId), IntHash<size_t>(key.Channel)), IntHash<size_t>(key.Hard));
    }
};

namespace {

class TWriter {
public:
    TWriter()
        : Info(TBlobStorageGroupType::Erasure4Plus2Block, 1, 4)
    {
        for (ui32 domain = 0; domain < 4; ++domain) {
            Caches.push_back(TIngressCache::Create(Info.PickTopology(), TVDiskID(0, 1, 0, domain, 0)));
        }
    }

    TIngressCachePtr GetCache() const {
        return Caches.front();
    }

    TBlobStorageGroupType GetGroupType() const {
        return Info.Type;
    }

    void Write(NBarriers::TTree& tree, bool gcOnlySynced, const TKeyBarrier& key, ui32 collectGen, ui32 collectStep) {
        for (const auto& cache : Caches) {
            tree.Update(gcOnlySynced, key, TMemRecBarrier(collectGen, collectStep, TBarrierIngress(cache.Get())));
        }
    }

    void Write(NBarriers::TMemView& memView, const TKeyBarrier& key, ui32 collectGen, ui32 collectStep) {
        for (const auto& cache : Caches) {
            memView.Update(key, TMemRecBarrier(collectGen, collectStep, TBarrierIngress(cache.Get())));
        }
    }

private:
    TBlobStorageGroupInfo Info;
    TVector<TIngressCachePtr> Caches;
};

class TOracle {
public:
    void Update(ui64 tabletId, ui32 channel, bool hard, ui32 collectGen, ui32 collectStep) {
        TBarrierPoint& point = Points[TOracleKey{tabletId, channel, hard}];
        if (!point.Dead && std::make_tuple(collectGen, collectStep) >= point.AsTuple()) {
            point.CollectGen = collectGen;
            point.CollectStep = collectStep;
            point.Dead = hard && collectGen == Max<ui32>() && collectStep == Max<ui32>();
        }
        if (point.Dead) {
            Points[TOracleKey{tabletId, channel, false}] = TBarrierPoint{Max<ui32>(), Max<ui32>(), true};
            Points[TOracleKey{tabletId, channel, true}] = TBarrierPoint{Max<ui32>(), Max<ui32>(), true};
        }
    }

    TMaybe<TBarrierPoint> Find(ui64 tabletId, ui32 channel, bool hard) const {
        auto it = Points.find(TOracleKey{tabletId, channel, hard});
        if (it == Points.end()) {
            return {};
        }
        return it->second;
    }

    bool KeepBlob(const TLogoBlobID& id, bool keepByIngress) const {
        const auto hard = Find(id.TabletID(), id.Channel(), true);
        const auto soft = Find(id.TabletID(), id.Channel(), false);
        const auto pos = std::make_tuple(id.Generation(), id.Step());
        const bool keepByHard = !hard || pos > hard->AsTuple();
        const bool keepBySoft = !soft || pos > soft->AsTuple();
        return keepByHard && (keepBySoft || keepByIngress);
    }

    void CheckTree(const NBarriers::TTree& tree, ui64 tabletId, ui32 channel) const {
        TMaybe<NBarriers::TCurrentBarrier> soft;
        TMaybe<NBarriers::TCurrentBarrier> hard;
        tree.GetBarrier(tabletId, channel, soft, hard);
        CheckBarrier(Find(tabletId, channel, false), soft);
        CheckBarrier(Find(tabletId, channel, true), hard);
    }

    void CheckEssence(const NGcOpt::TBarriersEssence& essence, const TLogoBlobID& id, bool keepFlag, bool allowGc) const {
        TIngress ingress;
        if (keepFlag) {
            ingress.SetKeep(TIngress::IngressMode(TBlobStorageGroupType::Erasure4Plus2Block), CollectModeKeep);
        }
        const auto status = essence.Keep(TKeyLogoBlob(id), TMemRecLogoBlob(ingress), {}, true, allowGc);
        const bool expected = !allowGc || KeepBlob(id, keepFlag);
        Y_ABORT_UNLESS(status.KeepData == expected);
        Y_ABORT_UNLESS(status.KeepIndex >= status.KeepData);
    }

private:
    void CheckBarrier(TMaybe<TBarrierPoint> expected, TMaybe<NBarriers::TCurrentBarrier> actual) const {
        Y_ABORT_UNLESS(bool(expected) == bool(actual));
        if (expected) {
            Y_ABORT_UNLESS(actual->CollectGen == expected->CollectGen);
            Y_ABORT_UNLESS(actual->CollectStep == expected->CollectStep);
        }
    }

    THashMap<TOracleKey, TBarrierPoint> Points;
};

TLogoBlobID MakeBlob(FuzzedDataProvider& provider) {
    const ui64 tabletId = TabletBase + provider.ConsumeIntegralInRange<ui32>(0, NumTablets - 1);
    const ui32 channel = provider.ConsumeIntegralInRange<ui32>(0, NumChannels - 1);
    const ui32 generation = provider.ConsumeIntegralInRange<ui32>(1, 32);
    const ui32 step = provider.ConsumeIntegralInRange<ui32>(0, 512);
    const ui32 cookie = provider.ConsumeIntegralInRange<ui32>(0, 1024);
    return TLogoBlobID(tabletId, generation, step, channel, 128, cookie);
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    FuzzedDataProvider provider(data, size);
    TWriter writer;
    const bool gcOnlySynced = provider.ConsumeBool();
    NBarriers::TTree tree(writer.GetCache(), VDiskLogPrefix);
    NBarriers::TMemView memView(writer.GetCache(), VDiskLogPrefix, gcOnlySynced);
    TBarrierCache cache;
    TOracle oracle;

    const ui32 ops = provider.ConsumeIntegralInRange<ui32>(0, MaxOps);
    for (ui32 i = 0; i < ops && provider.remaining_bytes(); ++i) {
        if (provider.ConsumeIntegralInRange<ui8>(0, 3) != 0) {
            const ui64 tabletId = TabletBase + provider.ConsumeIntegralInRange<ui32>(0, NumTablets - 1);
            const ui32 channel = provider.ConsumeIntegralInRange<ui32>(0, NumChannels - 1);
            const bool hard = provider.ConsumeBool();
            const ui32 gen = provider.ConsumeIntegralInRange<ui32>(1, 32);
            const ui32 genCounter = provider.ConsumeIntegralInRange<ui32>(0, 256);
            ui32 collectGen = provider.ConsumeIntegralInRange<ui32>(0, 32);
            ui32 collectStep = provider.ConsumeIntegralInRange<ui32>(0, 512);
            if (hard && provider.ConsumeIntegralInRange<ui8>(0, 63) == 0) {
                collectGen = Max<ui32>();
                collectStep = Max<ui32>();
            }

            const TKeyBarrier key(tabletId, channel, gen, genCounter, hard);
            writer.Write(tree, gcOnlySynced, key, collectGen, collectStep);
            writer.Write(memView, key, collectGen, collectStep);
            cache.Update(tabletId, channel, hard, collectGen, collectStep);
            oracle.Update(tabletId, channel, hard, collectGen, collectStep);
            oracle.CheckTree(tree, tabletId, channel);
        } else {
            const TLogoBlobID id = MakeBlob(provider);
            const bool keepByIngress = provider.ConsumeBool();
            TString explanation;
            Y_ABORT_UNLESS(cache.Keep(id, keepByIngress, &explanation) == oracle.KeepBlob(id, keepByIngress));

            NGcOpt::TBarriersEssence essence(memView.GetSnapshot(), writer.GetGroupType());
            oracle.CheckEssence(essence, id, keepByIngress, provider.ConsumeBool());
        }
    }

    return 0;
}
