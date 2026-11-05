#pragma once

#include "defs.h"

#include <ydb/core/protos/bridge.pb.h>

namespace NKikimr::NBsController {

    // bridge pile sync progress, labelled by pile, database and storage pool
    class TBridgeSyncCounters {
    public:
        using TKey = std::tuple<TString, TString>; // pile name, storage pool name

        struct TPileValues {
            NKikimrBridge::TClusterState::EPileState State = {};
            bool UnsyncedBSC = false;
        };

        struct TValues {
            std::array<ui64, NKikimrBridge::TGroupState::EStage_ARRAYSIZE> GroupsInStage{};
            ui64 SyncersRunning = 0;
            ui64 BytesDone = 0;
            ui64 BytesTotal = 0;
        };

    private:
        struct TPileCounters {
            std::array<::NMonitoring::TDynamicCounters::TCounterPtr, NKikimrBridge::TClusterState::EPileState_ARRAYSIZE> InState;
            ::NMonitoring::TDynamicCounters::TCounterPtr UnsyncedBSC;

            TPileCounters(const ::NMonitoring::TDynamicCounterPtr& counters)
                : UnsyncedBSC(counters->GetCounter("UnsyncedBSC"))
            {
                for (size_t i = 0; i < InState.size(); ++i) {
                    InState[i] = counters->GetSubgroup("state", NKikimrBridge::TClusterState::EPileState_Name(i))
                        ->GetCounter("PileInState");
                }
            }
        };

        struct TPoolCounters {
            std::array<::NMonitoring::TDynamicCounters::TCounterPtr, NKikimrBridge::TGroupState::EStage_ARRAYSIZE> GroupsInStage;
            ::NMonitoring::TDynamicCounters::TCounterPtr SyncersRunning;
            ::NMonitoring::TDynamicCounters::TCounterPtr SyncBytesDone;
            ::NMonitoring::TDynamicCounters::TCounterPtr SyncBytesTotal;
            ::NMonitoring::TDynamicCounters::TCounterPtr SyncErrors;

            TPoolCounters(const ::NMonitoring::TDynamicCounterPtr& counters)
                : SyncersRunning(counters->GetCounter("SyncersRunning"))
                , SyncBytesDone(counters->GetCounter("SyncBytesDone"))
                , SyncBytesTotal(counters->GetCounter("SyncBytesTotal"))
                , SyncErrors(counters->GetCounter("SyncErrors", true))
            {
                for (size_t i = 0; i < GroupsInStage.size(); ++i) {
                    GroupsInStage[i] = counters->GetSubgroup("stage", NKikimrBridge::TGroupState::EStage_Name(i))
                        ->GetCounter("GroupsInStage");
                }
            }
        };

        ::NMonitoring::TDynamicCounterPtr Root;
        ::NMonitoring::TDynamicCounterPtr Counters;
        std::map<TString, TPileCounters> Piles;
        std::map<TKey, TPoolCounters> Pools;

    public:
        TBridgeSyncCounters(::NMonitoring::TDynamicCounterPtr root)
            : Root(std::move(root))
            , Counters(Root->GetSubgroup("subsystem", "bsc"))
        {}

        ~TBridgeSyncCounters() {
            Root->RemoveSubgroup("subsystem", "bsc");
        }

        void IncErrors(const TKey& key) {
            GetPool(key).SyncErrors->Inc();
        }

        void Update(const std::map<TString, TPileValues>& piles, const std::map<TKey, TValues>& values) {
            for (auto it = Pools.begin(); it != Pools.end(); ) {
                if (values.contains(it->first)) {
                    ++it;
                } else {
                    const auto& [pile, pool] = it->first;
                    Counters->RemoveSubgroupChain({{"pile", pile}, {"database", GetDatabase(pool)}, {"storagePool", pool}});
                    it = Pools.erase(it);
                }
            }
            for (auto it = Piles.begin(); it != Piles.end(); ) {
                if (piles.contains(it->first)) {
                    ++it;
                } else {
                    Counters->RemoveSubgroup("pile", it->first);
                    it = Piles.erase(it);
                }
            }

            for (const auto& [name, value] : piles) {
                auto it = Piles.find(name);
                if (it == Piles.end()) {
                    it = Piles.try_emplace(name, Counters->GetSubgroup("pile", name)).first;
                }
                for (size_t i = 0; i < it->second.InState.size(); ++i) {
                    it->second.InState[i]->Set(static_cast<size_t>(value.State) == i);
                }
                it->second.UnsyncedBSC->Set(value.UnsyncedBSC);
            }
            for (const auto& [key, value] : values) {
                auto& pool = GetPool(key);
                for (size_t i = 0; i < value.GroupsInStage.size(); ++i) {
                    pool.GroupsInStage[i]->Set(value.GroupsInStage[i]);
                }
                pool.SyncersRunning->Set(value.SyncersRunning);
                pool.SyncBytesDone->Set(value.BytesDone);
                pool.SyncBytesTotal->Set(value.BytesTotal);
            }
        }

    private:
        TPoolCounters& GetPool(const TKey& key) {
            auto it = Pools.find(key);
            if (it == Pools.end()) {
                const auto& [pile, pool] = key;
                it = Pools.try_emplace(key, Counters->GetSubgroup("pile", pile)
                    ->GetSubgroup("database", GetDatabase(pool))
                    ->GetSubgroup("storagePool", pool)).first;
            }
            return it->second;
        }

        // console names database storage pools as "<database>:<kind>"
        static TString GetDatabase(const TString& pool) {
            return TString(TStringBuf(pool).RBefore(':'));
        }
    };

} // NKikimr::NBsController
