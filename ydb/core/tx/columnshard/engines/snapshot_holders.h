#pragma once

#include "portions/portion_info.h"

#include <ydb/core/tx/columnshard/common/path_id.h>
#include <ydb/core/tx/long_tx_service/public/snapshot_registry.h>

#include <algorithm>

namespace NKikimr::NColumnShard {
class TTableInfo;
}

namespace NKikimr::NOlap {

/**
The life looks like this:
time ---------[----------------------------------------]--------->
        ^scan1  ^minSnapshotForNewReads  ^scan2 ^scan3   ^now

In this class, we need only minSnapshotForNewReads and scan1.
We do not need scan2 and scan3 because we consider the whole range [minSnapshotForNewReads, now] as "in flight",
because any number of new scans may come there.
*/
class TSnapshotHoldersPerTable {
    // It is the oldest snapshot new scans can start at
    const TSnapshot MinSnapshotForNewReads;
    // Sorted from older to younger
    const std::vector<TSnapshot> TxInFlight;

public:
    TSnapshotHoldersPerTable(TSnapshot minSnapshotForNewReads, std::vector<TSnapshot> txInFlight)
        : MinSnapshotForNewReads(std::move(minSnapshotForNewReads))
        , TxInFlight(std::move(txInFlight))
    {
        AFL_VERIFY(std::is_sorted(TxInFlight.begin(), TxInFlight.end()));
        if (!TxInFlight.empty()) {
            AFL_VERIFY(TxInFlight.back() < MinSnapshotForNewReads);
        }
    }

    TSnapshot GetMinSnapshotForNewReads() const {
        return MinSnapshotForNewReads;
    }

    bool CouldUsePortion(const TPortionInfo::TConstPtr& portion) const {
        // The object can be used by new scans.
        if (!portion->IsRemovedFor(MinSnapshotForNewReads)) {
            return true;
        }

        // Loop invariant: all txs older than txSnapshot couldn't use the object.
        for (const auto& txSnapshot : TxInFlight) {
            // This and all younger txs cannot see it.
            if (portion->IsRemovedFor(txSnapshot)) {
                return false;
            }
            // This tx could use it.
            if (portion->IsVisible(txSnapshot)) {
                return true;
            }
            // Current tx could not use it, maybe the next tx could.
        }
        // We have not found txs that could use it.
        return false;
    }
};

class ISnapshotHolders {
public:
    virtual ~ISnapshotHolders() = default;
    virtual TSnapshot GetMinSnapshotForNewReads() const = 0;
    virtual bool CouldUsePortion(const TPortionInfo::TConstPtr& portion) const = 0;
};

class TLegacySnapshotHolders: public ISnapshotHolders {
    TSnapshotHoldersPerTable impl;

public:
    TLegacySnapshotHolders(TSnapshot minSnapshotForNewReads, std::vector<TSnapshot> txInFlight)
        : impl(minSnapshotForNewReads, std::move(txInFlight))
    {
    }

    TSnapshot GetMinSnapshotForNewReads() const override {
        return impl.GetMinSnapshotForNewReads();
    }

    bool CouldUsePortion(const TPortionInfo::TConstPtr& portion) const override {
        return impl.CouldUsePortion(portion);
    }
};

class TRegistrySnapshotHolders: public ISnapshotHolders {
private:
    const TSnapshot MinSnapshotForNewReads;
    const TTrueAtomicSharedPtr<IImmutableSnapshotRegistry> Registry;
    const ui64 SchemeShardId;
    const IPathIdTranslator& PathIdTranslator;
    mutable THashMap<TInternalPathId, TSnapshotHoldersPerTable> HoldersByPathId;

private:
    TSnapshotHoldersPerTable BuildHoldersForTable(const std::set<NColumnShard::TSchemeShardLocalPathId>& schemeShardLocalPathIds) const;
    const TSnapshotHoldersPerTable& GetHoldersByPathId(const TInternalPathId pathId) const;

public:
    TRegistrySnapshotHolders(const TSnapshot minSnapshotForNewReads, TTrueAtomicSharedPtr<IImmutableSnapshotRegistry> registry,
        const ui64 schemeShardId, const IPathIdTranslator& pathIdTranslator);

    TSnapshot GetMinSnapshotForNewReads() const override {
        return MinSnapshotForNewReads;
    }

    bool CouldUsePortion(const TPortionInfo::TConstPtr& portion) const override;
};

}   // namespace NKikimr::NOlap
