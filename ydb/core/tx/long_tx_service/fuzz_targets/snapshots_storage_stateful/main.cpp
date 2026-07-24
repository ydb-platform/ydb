#include <ydb/core/base/appdata.h>
#include <ydb/core/protos/long_tx_service_config.pb.h>
#include <ydb/core/tx/long_tx_service/snapshots_storage.h>
#include <ydb/library/actors/testlib/test_runtime.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <library/cpp/time_provider/time_provider.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/vector.h>
#include <util/system/yassert.h>

#include <algorithm>
#include <atomic>

namespace {

using namespace NKikimr;
using namespace NKikimr::NLongTxService;
using namespace NActors;

constexpr size_t MaxSteps = 240;

class TFuzzActorRuntime final : public TTestActorRuntimeBase {
public:
    TFuzzActorRuntime()
        : TTestActorRuntimeBase(1, false)
    {
        TAppData::TimeProvider = TimeProvider;
        Initialize();
    }

protected:
    void InitNodeImpl(TNodeDataBase* node, size_t nodeIndex) override {
        node->AppData0.reset(new TAppData(0, 0, 0, 0, {}, nullptr, nullptr, nullptr, nullptr));
        TTestActorRuntimeBase::InitNodeImpl(node, nodeIndex);
    }
};

TActorId Actor(ui32 node, ui64 id) {
    return TActorId(node, id);
}

TRowVersion Version(FuzzedDataProvider& provider) {
    return TRowVersion(
        provider.ConsumeIntegralInRange<ui64>(0, 32),
        provider.ConsumeIntegralInRange<ui64>(0, 128));
}

TVector<TTableId> Tables(FuzzedDataProvider& provider) {
    TVector<TTableId> result;
    const size_t count = provider.ConsumeIntegralInRange<size_t>(0, 4);
    for (size_t i = 0; i < count; ++i) {
        result.emplace_back(
            provider.ConsumeIntegralInRange<ui64>(1, 4),
            provider.ConsumeIntegralInRange<ui64>(1, 32));
    }
    return result;
}

struct TLocalModel {
    TRowVersion Snapshot;
    TActorId Session;
    std::shared_ptr<std::atomic<bool>> Alive;
};

struct TRemoteModel {
    TRowVersion Snapshot;
    TActorId Session;
};

struct TRemoteNodeModel {
    TInstant CollectionTime;
    TVector<TRemoteModel> Snapshots;
};

TVector<std::tuple<TRowVersion, TActorId>> CollectLocal(const TLocalSnapshotsStorage& storage) {
    TVector<std::tuple<TRowVersion, TActorId>> result;
    for (const auto& snapshot : storage.View()) {
        result.emplace_back(snapshot.Snapshot, snapshot.SessionActorId);
    }
    Sort(result);
    return result;
}

TVector<std::tuple<TRowVersion, TActorId>> ExpectedLocal(const TVector<TLocalModel>& model, TInstant now, ui64 promotionSeconds) {
    TVector<std::tuple<TRowVersion, TActorId>> result;
    const ui64 promotionMs = TDuration::Seconds(promotionSeconds).MilliSeconds();
    const ui64 nowMs = now.MilliSeconds();
    const ui64 maxSnapshotStep = nowMs > promotionMs ? nowMs - promotionMs : 0;
    for (const auto& snapshot : model) {
        if (snapshot.Alive->load() && snapshot.Snapshot.Step <= maxSnapshotStep) {
            result.emplace_back(snapshot.Snapshot, snapshot.Session);
        }
    }
    Sort(result);
    return result;
}

TVector<std::tuple<TRowVersion, TActorId>> CollectRemote(const TRemoteSnapshotsStorage& storage) {
    TVector<std::tuple<TRowVersion, TActorId>> result;
    for (const auto& snapshot : storage.View()) {
        result.emplace_back(snapshot.Snapshot, snapshot.SessionActorId);
    }
    Sort(result);
    return result;
}

TVector<std::tuple<TRowVersion, TActorId>> ExpectedRemote(const THashMap<ui32, TRemoteNodeModel>& model) {
    TVector<std::tuple<TRowVersion, TActorId>> result;
    for (const auto& [_, state] : model) {
        for (const auto& snapshot : state.Snapshots) {
            result.emplace_back(snapshot.Snapshot, snapshot.Session);
        }
    }
    Sort(result);
    return result;
}

void Check(
    const TLocalSnapshotsStorage& local,
    const TRemoteSnapshotsStorage& remote,
    const TVector<TLocalModel>& localModel,
    const THashMap<ui32, TRemoteNodeModel>& remoteModel,
    TInstant now,
    ui64 promotionSeconds,
    TRowVersion border,
    bool ready)
{
    Y_ABORT_UNLESS(CollectLocal(local) == ExpectedLocal(localModel, now, promotionSeconds));
    Y_ABORT_UNLESS(CollectRemote(remote) == ExpectedRemote(remoteModel));
    Y_ABORT_UNLESS(remote.GetBorder() == border);
    Y_ABORT_UNLESS(remote.IsReady() == ready);
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    FuzzedDataProvider provider(data, size);
    static TFuzzActorRuntime runtime;

    runtime.RunCall([&] {
        const ui64 promotionSeconds = provider.ConsumeIntegralInRange<ui64>(0, 8);
        const ui64 expirationSeconds = provider.ConsumeIntegralInRange<ui64>(1, 16);
        AppData()->LongTxServiceConfig.SetLocalSnapshotPromotionTimeSeconds(promotionSeconds);
        AppData()->LongTxServiceConfig.SetUnavailableNodeSnapshotsExpirationTimeSeconds(expirationSeconds);

        TLocalSnapshotsStorage local;
        TRemoteSnapshotsStorage remote;
        TVector<TLocalModel> localModel;
        THashMap<ui32, TRemoteNodeModel> remoteModel;
        TRowVersion border = TRowVersion::Max();
        bool ready = false;
        ui64 nowSeconds = 1;

        for (size_t step = 0; step < MaxSteps && provider.remaining_bytes(); ++step) {
            nowSeconds += provider.ConsumeIntegralInRange<ui64>(0, 4);
            TAppData::TimeProvider = CreateDeterministicTimeProvider(nowSeconds);
            const TInstant now = TInstant::Seconds(nowSeconds);

            switch (provider.ConsumeIntegralInRange<ui8>(0, 6)) {
                case 0: {
                    TLocalSnapshotInfo snapshot(
                        Version(provider),
                        Actor(1, provider.ConsumeIntegralInRange<ui64>(1, 64)),
                        Tables(provider));
                    const bool isNew = std::none_of(localModel.begin(), localModel.end(), [&](const auto& item) {
                        return item.Snapshot == snapshot.Snapshot && item.Session == snapshot.SessionActorId;
                    });
                    if (isNew) {
                        localModel.push_back({snapshot.Snapshot, snapshot.SessionActorId, snapshot.AliveFlag});
                    }
                    local.Insert(std::move(snapshot));
                    break;
                }
                case 1: {
                    if (!localModel.empty()) {
                        auto& snapshot = localModel[provider.ConsumeIntegralInRange<size_t>(0, localModel.size() - 1)];
                        snapshot.Alive->store(provider.ConsumeBool());
                    }
                    break;
                }
                case 2: {
                    local.CleanExpired();
                    localModel.erase(
                        std::remove_if(localModel.begin(), localModel.end(), [](const auto& x) { return !x.Alive->load(); }),
                        localModel.end());
                    break;
                }
                case 3: {
                    const size_t nodes = provider.ConsumeIntegralInRange<size_t>(0, 4);
                    THashMap<ui32, TInstant> updates;
                    TVector<TRemoteSnapshotInfo> snapshots;
                    THashSet<ui32> updatedNodes;
                    for (size_t i = 0; i < nodes; ++i) {
                        const ui32 nodeId = provider.ConsumeIntegralInRange<ui32>(1, 8);
                        const TInstant collection = now - TDuration::Seconds(provider.ConsumeIntegralInRange<ui64>(0, 12));
                        updates[nodeId] = collection;
                    }
                    for (const auto& [nodeId, _] : updates) {
                        const size_t count = provider.ConsumeIntegralInRange<size_t>(0, 4);
                        for (size_t i = 0; i < count; ++i) {
                            snapshots.emplace_back(
                                Version(provider),
                                Actor(nodeId, provider.ConsumeIntegralInRange<ui64>(1, 128)),
                                Tables(provider));
                        }
                    }
                    remote.UpdateAndCleanExpired(snapshots, updates);

                    TVector<ui32> expired;
                    for (const auto& [nodeId, state] : remoteModel) {
                        if (!updates.contains(nodeId) && state.CollectionTime + TDuration::Seconds(expirationSeconds) < now) {
                            expired.push_back(nodeId);
                        }
                    }
                    for (ui32 nodeId : expired) {
                        remoteModel.erase(nodeId);
                    }
                    for (const auto& [nodeId, collection] : updates) {
                        auto& state = remoteModel[nodeId];
                        if (state.CollectionTime < collection) {
                            state.CollectionTime = collection;
                            state.Snapshots.clear();
                            for (const auto& snapshot : snapshots) {
                                if (snapshot.SessionActorId.NodeId() == nodeId) {
                                    state.Snapshots.push_back({snapshot.Snapshot, snapshot.SessionActorId});
                                }
                            }
                        }
                    }
                    ready = true;
                    break;
                }
                case 4: {
                    border = Version(provider);
                    remote.UpdateBorder(border);
                    break;
                }
                case 5: {
                    local.Clear();
                    localModel.clear();
                    break;
                }
                case 6: {
                    remote.Clear();
                    remoteModel.clear();
                    border = TRowVersion::Max();
                    ready = false;
                    break;
                }
            }

            Check(local, remote, localModel, remoteModel, now, promotionSeconds, border, ready);
        }

        return true;
    });

    return 0;
}
