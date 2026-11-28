#include "scan.h"

#include <ydb/core/tx/columnshard/data_accessor/cache_policy/policy.h>
#include <ydb/core/tx/columnshard/engines/storage/actualizer/move/move.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NColumnShard {
namespace {
using TCachePolicy = NOlap::NGeneralCache::TPortionsMetadataCachePolicy;
using TAddress = TCachePolicy::TAddress;
using TCache = NGeneralCache::TServiceOperator<TCachePolicy>;

struct TEvAccessorsReady: NActors::TEventLocal<TEvAccessorsReady, EventSpaceBegin(TEvents::ES_PRIVATE) + 1> {
    THashMap<TAddress, TCachePolicy::TObject> Accessors;

    TEvAccessorsReady(THashMap<TAddress, TCachePolicy::TObject>&& accessors)
        : Accessors(std::move(accessors))
    {
    }
};

class TMetadataCallback: public TCache::ICallback {
    TActorId Owner;
    std::shared_ptr<TAtomicCounter> Aborted;

    bool DoIsAborted() const override {
        return Aborted->Val();
    }

    void DoOnResultReady(THashMap<TCachePolicy::TAddress, TCachePolicy::TObject>&& accessors, THashSet<TCachePolicy::TAddress>&&,
        NGeneralCache::NPublic::TErrorAddresses<TCachePolicy>&&) override {
        TActivationContext::Send(Owner, std::make_unique<TEvAccessorsReady>(std::move(accessors)));
    }

public:
    TMetadataCallback(TActorId owner, const std::shared_ptr<TAtomicCounter>& aborted)
        : Owner(owner)
        , Aborted(aborted)
    {
    }
};

class TMoveDataScan: public TActorBootstrapped<TMoveDataScan> {
    const TActorId Owner;
    std::vector<TMoveDataScanPortion> Portions;
    const THashSet<ui32> Groups;
    const ui64 MemoryLimit;
    std::vector<TMoveDataScanPortion> Batch;
    std::vector<TMoveDataScanPortion> Retry;
    const std::shared_ptr<TAtomicCounter> Aborted = std::make_shared<TAtomicCounter>(0);

    void RequestBatch() {
        THashSet<TAddress> addresses;
        ui64 memory = 0;
        while (!Portions.empty() && Batch.size() < 1024 && (Batch.empty() || memory < MemoryLimit)) {
            const auto portion = Portions.back();
            Portions.pop_back();
            addresses.emplace(Owner, NOlap::TPortionAddress(portion.PathId, portion.PortionId));
            Batch.push_back(portion);
            memory += portion.AccessorMemory;
        }
        AFL_VERIFY(!addresses.empty());
        TCache::AskObjects(TCachePolicy::EConsumer::MOVE_DATA, std::move(addresses), std::make_shared<TMetadataCallback>(SelfId(), Aborted));
    }

    void Handle(TEvAccessorsReady::TPtr& ev) {
        auto result = std::make_unique<TEvMoveData::TEvBatch>();
        for (const auto& portion : Batch) {
            const TAddress key(Owner, NOlap::TPortionAddress(portion.PathId, portion.PortionId));
            const auto* accessor = ev->Get()->Accessors.FindPtr(key);
            if (!accessor) {
                result->Missing.push_back(portion);
            } else if (NOlap::NActualizer::TMoveDataActualizer::HasBlobInGroups((*accessor)->GetBlobIds(), Groups)) {
                result->Matches.push_back(portion);
            } else {
                ++result->Rejected;
            }
        }
        Batch.clear();
        Send(Owner, result.release());
    }

    void Handle(TEvMoveData::TEvContinue::TPtr& ev) {
        Retry.insert(Retry.end(), ev->Get()->Retry.begin(), ev->Get()->Retry.end());
        if (!Portions.empty()) {
            RequestBatch();
        } else if (!Retry.empty()) {
            Portions.swap(Retry);
            Schedule(TDuration::Seconds(5), new TEvents::TEvWakeup());
        } else {
            PassAway();
        }
    }

    void PassAway() override {
        Aborted->Inc();
        TActorBootstrapped::PassAway();
    }

public:
    TMoveDataScan(TActorId owner, std::vector<TMoveDataScanPortion>&& portions, const THashSet<ui32>& groups, ui64 memoryLimit)
        : Owner(owner)
        , Portions(std::move(portions))
        , Groups(groups)
        , MemoryLimit(memoryLimit)
    {
    }

    void Bootstrap() {
        Become(&TThis::StateWork);
        RequestBatch();
    }

    STRICT_STFUNC(StateWork,
        hFunc(TEvAccessorsReady, Handle);
        hFunc(TEvMoveData::TEvContinue, Handle);
        cFunc(TEvents::TEvWakeup::EventType, RequestBatch);
        cFunc(TEvents::TEvPoisonPill::EventType, PassAway);)
};
}   // namespace

NActors::IActor* CreateMoveDataScan(
    TActorId owner, std::vector<TMoveDataScanPortion>&& portions, const THashSet<ui32>& groups, ui64 memoryLimit) {
    return new TMoveDataScan(owner, std::move(portions), groups, memoryLimit);
}

}   // namespace NKikimr::NColumnShard
