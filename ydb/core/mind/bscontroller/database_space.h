#pragma once

#include "defs.h"
#include "types.h"

#include <ydb/core/protos/blobstorage_disk_color.pb.h>
#include <ydb/core/scheme/scheme_pathid.h>

namespace NKikimr::NBsController {

    // Keeps track of space color of groups in every storage pool bound to a database (via pool's ScopeId) and decides
    // whether the database is running out of space. The pool is exhausted when every physical group in it has reached
    // the block color; it stops being exhausted when some group gets better than the unblock color. A group's color is
    // the worst color of its VDisks, so while not every VDisk has reported its metrics (e.g. after reassignment or
    // restart), only a lower bound is known: it is enough to tell the group has reached the block color, but not that
    // it is better than the unblock one. The database is exhausted when any of its pools is. Interested nodes
    // subscribe to databases and get notified whenever this changes.
    class TDatabaseSpaceTracker {
    public:
        using ESpaceColor = NKikimrBlobStorage::TPDiskSpaceColor::E;
        using TScope = TPathId; // the database's domain key, as in ScopeId of its storage pools

    private:
        struct TPool {
            std::optional<TScope> Scope;
            std::map<ESpaceColor, ui32> Colors; // number of groups by known (lower bound of) color; zero entries are not kept
            std::map<ESpaceColor, ui32> CompleteColors; // the same for groups with every VDisk reported
            ui32 NumUnknownGroups = 0; // groups whose color is not known at all
            ui32 NumIncompleteGroups = 0; // groups not every VDisk of which has reported (including unknown ones)
            ui32 NumGroups = 0;
            bool Exhausted = false;
        };

        struct TGroup {
            TBoxStoragePoolId PoolId;
            std::optional<ESpaceColor> Color; // the worst color of VDisks reported, nullopt when none has
            bool Complete = false; // all the VDisks of the group have reported
        };

        ESpaceColor BlockColor = NKikimrBlobStorage::TPDiskSpaceColor::GREEN; // GREEN means disabled
        ESpaceColor UnblockColor = NKikimrBlobStorage::TPDiskSpaceColor::GREEN; // GREEN means the same as BlockColor
        std::map<TBoxStoragePoolId, TPool> Pools;
        THashMap<TGroupId, TGroup> Groups;
        std::map<TScope, std::set<TBoxStoragePoolId>> ScopePools;
        std::set<TScope> ChangedScopes;
        std::set<TBoxStoragePoolId> ChangedPools; // pools whose colors or state might have changed, for monitoring
        std::map<TBoxStoragePoolId, bool> ChangedLatches; // existing pools whose Exhausted state has changed, to be persisted

        ui32 BatchDepth = 0;
        std::set<TBoxStoragePoolId> DirtyPools; // pools to recalculate when the batch ends

        std::map<TScope, std::set<TNodeId>> Subscribers;
        std::map<TNodeId, std::set<TScope>> NodeSubscriptions;

    public:
        // Pools are recalculated when the batch ends, against their complete counters: with hysteresis, evaluating
        // intermediate states of several changes applied one by one could leave a pool in a state that depends on the
        // order of changes.
        class TBatch {
            TDatabaseSpaceTracker& Tracker;

        public:
            explicit TBatch(TDatabaseSpaceTracker& tracker)
                : Tracker(tracker)
            {
                ++Tracker.BatchDepth;
            }

            ~TBatch() {
                if (!--Tracker.BatchDepth) {
                    Tracker.RecalculateDirtyPools();
                }
            }
        };

        static bool IsValidThresholds(ESpaceColor block, ESpaceColor unblock);
        void SetThresholds(ESpaceColor block, ESpaceColor unblock);

        void SetPool(TBoxStoragePoolId poolId, std::optional<TScope> scope);
        void RemovePool(TBoxStoragePoolId poolId);

        // restore the persisted hysteresis latch of the pool (before its groups are added)
        void RestorePoolExhausted(TBoxStoragePoolId poolId, bool exhausted);

        // returns existing pools whose Exhausted state has changed since the last call
        std::map<TBoxStoragePoolId, bool> TakeChangedLatches() { return std::exchange(ChangedLatches, {}); }

        // set physical group's color: the worst color of its VDisks that have reported (nullopt when none has) and
        // whether all of them have; groups that are not physical ones are not to be added (or are to be removed)
        void SetGroup(TGroupId groupId, TBoxStoragePoolId poolId, std::optional<ESpaceColor> color, bool complete = true);
        void RemoveGroup(TGroupId groupId);

        // forget everything except thresholds and subscriptions
        void ResetState();

        bool IsExhausted(TScope scope) const;

        struct TPoolState {
            std::optional<ESpaceColor> BestColor; // unset when the pool has no physical groups
            std::optional<ESpaceColor> WorstColor;
            ui32 NumGroups = 0;
            ui32 NumUnknownGroups = 0; // groups with no VDisk reported
            ui32 NumIncompleteGroups = 0; // groups with not every VDisk reported (including unknown ones)
            bool Exhausted = false;
        };
        std::optional<TPoolState> GetPoolState(TBoxStoragePoolId poolId) const;

        ESpaceColor GetBlockColor() const { return BlockColor; }
        ESpaceColor GetUnblockColor() const { return UnblockColor; }
        ESpaceColor GetEffectiveUnblockColor() const;
        const std::map<TScope, std::set<TBoxStoragePoolId>>& GetScopePools() const { return ScopePools; }
        const std::map<TScope, std::set<TNodeId>>& GetSubscribers() const { return Subscribers; }

        // returns scopes whose state has changed since the last call and have subscribers
        std::vector<TScope> TakeChangedScopes();

        // returns pools whose colors or state might have changed since the last call
        std::set<TBoxStoragePoolId> TakeChangedPools() { return std::exchange(ChangedPools, {}); }

        void Subscribe(TNodeId nodeId, const std::vector<TScope>& scopes);
        void Unsubscribe(TNodeId nodeId, const std::vector<TScope>& scopes);
        void UnsubscribeNode(TNodeId nodeId);
        const std::set<TNodeId> *GetSubscribers(TScope scope) const;

    private:
        // update pool's counters without recalculating its state
        void CountGroup(const TGroup& group);
        void UncountGroup(const TGroup& group);
        void Recalculate(TBoxStoragePoolId poolId); // now or when the batch ends
        void RecalculateDirtyPools();
        void RecalculatePool(TBoxStoragePoolId poolId, TPool& pool);
        void MarkChanged(const TPool& pool);
    };

} // NKikimr::NBsController
