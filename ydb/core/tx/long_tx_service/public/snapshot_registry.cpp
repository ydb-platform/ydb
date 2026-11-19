#include "snapshot_registry.h"

#include <ydb/library/actors/core/log.h>

#include <algorithm>

namespace NKikimr {

namespace {
    NKikimr::TTableId NormalizeTableId(const NKikimr::TTableId& tableId) {
        if (tableId.SchemaVersion == 0 && tableId.SysViewInfo.empty()) {
            return tableId;
        }
        // Snapshot registry keys are path-based and intentionally ignore schema version
        // and sys-view suffixes to keep lookup/add behavior stable for all callers.
        return NKikimr::TTableId(tableId.PathId);
    }

    class TImmutableSnapshotRegistry : public IImmutableSnapshotRegistry {
    public:
        using TSnapshotMap = THashMap<NKikimr::TTableId, TVector<TRowVersion>>;

        TImmutableSnapshotRegistry() = default;
        
        explicit TImmutableSnapshotRegistry(TSnapshotMap&& snapshots, TRowVersion snapshotBorder, TInstant oldestCollectionTime)
            : Snapshots(std::move(snapshots))
            , SnapshotBorder(snapshotBorder)
            , OldestCollectionTime(oldestCollectionTime)
        {}

        ~TImmutableSnapshotRegistry() override = default;
        
        bool HasSnapshot(const NKikimr::TTableId& tableId, const TRowVersion& version) const override {
            if (version >= SnapshotBorder) {
                return true;
            }
            const NKikimr::TTableId normalizedTableId = NormalizeTableId(tableId);
            return HasSnapshotImpl(normalizedTableId, version) || HasSnapshotImpl(NKikimr::TTableId{}, version);
        }

        TSet<TRowVersion> GetActiveSnapshots(const NKikimr::TTableId& tableId) const override {
            TSet<TRowVersion> result;
            const NKikimr::TTableId normalizedTableId = NormalizeTableId(tableId);
            AddSnapshotsImpl(normalizedTableId, result);
            AddSnapshotsImpl(NKikimr::TTableId{}, result);
            return result;
        }

        void ForEachActiveSnapshot(const NKikimr::TTableId& tableId,
            const TRowVersion& from, const TRowVersion& to,
            TFunctionRef<void(const TRowVersion&)> visitor) const override
        {
            if (from >= to) {
                return;
            }

            const auto& tableVersions = GetSnapshotVersions(NormalizeTableId(tableId));
            const auto& allTableVersions = GetSnapshotVersions(NKikimr::TTableId{});
            auto tableIt = std::lower_bound(tableVersions.begin(), tableVersions.end(), from);
            const auto tableEnd = std::lower_bound(tableIt, tableVersions.end(), to);
            auto allIt = std::lower_bound(allTableVersions.begin(), allTableVersions.end(), from);
            const auto allEnd = std::lower_bound(allIt, allTableVersions.end(), to);

            while (tableIt != tableEnd || allIt != allEnd) {
                const TRowVersion version = tableIt == tableEnd ? *allIt
                    : allIt == allEnd ? *tableIt : std::min(*tableIt, *allIt);
                visitor(version);
                while (tableIt != tableEnd && *tableIt == version) {
                    ++tableIt;
                }
                while (allIt != allEnd && *allIt == version) {
                    ++allIt;
                }
            }
        }

        TRowVersion GetBorder() const override {
            return SnapshotBorder;
        }

        TInstant GetOldestCollectionTime() const override {
            return OldestCollectionTime;
        }
        
    private:
        const TVector<TRowVersion>& GetSnapshotVersions(const NKikimr::TTableId& tableId) const {
            if (const auto it = Snapshots.find(tableId); it != Snapshots.end()) {
                return it->second;
            }
            static const TVector<TRowVersion> empty;
            return empty;
        }

        bool HasSnapshotImpl(const NKikimr::TTableId& tableId, const TRowVersion& version) const {
            auto snapshotsIter = Snapshots.find(tableId);
            if (snapshotsIter == Snapshots.end()) {
                return false;
            }
            
            const auto& versions = snapshotsIter->second;

            auto versionsIter = std::lower_bound(versions.begin(), versions.end(), version);
            return versionsIter != versions.end() && *versionsIter == version;
        }

        void AddSnapshotsImpl(const NKikimr::TTableId& tableId, TSet<TRowVersion>& result) const {
            auto snapshotsIter = Snapshots.find(tableId);
            if (snapshotsIter == Snapshots.end()) {
                return;
            }

            result.insert(snapshotsIter->second.begin(), snapshotsIter->second.end());
        }

        const TSnapshotMap Snapshots;
        const TRowVersion SnapshotBorder;
        const TInstant OldestCollectionTime;
    };

    class TImmutableSnapshotRegistryHolder : public IImmutableSnapshotRegistryHolder {
    public:
        TImmutableSnapshotRegistryHolder() = default;

        ~TImmutableSnapshotRegistryHolder() override = default;

        const TTrueAtomicSharedPtr<IImmutableSnapshotRegistry>& Get() const override {
            return Registry;
        }

        void Set(std::unique_ptr<IImmutableSnapshotRegistry>&& registry) override {
            TTrueAtomicSharedPtr<IImmutableSnapshotRegistry> newRegistry;
            if (registry) {
                newRegistry = TTrueAtomicSharedPtr<IImmutableSnapshotRegistry>(registry.release());
            }
            Registry.swap(newRegistry);
        }
    private:
        TTrueAtomicSharedPtr<IImmutableSnapshotRegistry> Registry;
    };

    class TImmutableSnapshotRegistryBuilder : public IImmutableSnapshotRegistryBuilder {
    public:
        TImmutableSnapshotRegistryBuilder() = default;

        ~TImmutableSnapshotRegistryBuilder() override = default;

        void SetSnapshotBorder(const TRowVersion& version) override {
            SnapshotBorder = version;
        }

        void SetOldestCollectionTime(TInstant oldestCollectionTime) override {
            OldestCollectionTime = oldestCollectionTime;
        }
        
        void AddSnapshot(const TVector<NKikimr::TTableId>& tableIds, const TRowVersion& version) override {
            if (version >= SnapshotBorder) {
                return;
            }

            if (tableIds.empty()) {
                Snapshots[NKikimr::TTableId{}].push_back(version);
            }

            for (const NKikimr::TTableId& tableId : tableIds) {
                Snapshots[NormalizeTableId(tableId)].push_back(version);
            }
        }
        
        std::unique_ptr<IImmutableSnapshotRegistry> Build() && override {
            for (auto& [tableId, versions] : Snapshots) {
                std::sort(versions.begin(), versions.end());
            }
            return std::make_unique<TImmutableSnapshotRegistry>(std::move(Snapshots), SnapshotBorder, OldestCollectionTime);
        }

    private:
        TRowVersion SnapshotBorder = TRowVersion::Max();
        TInstant OldestCollectionTime;
        THashMap<NKikimr::TTableId, TVector<TRowVersion>> Snapshots;
    };
}

void IImmutableSnapshotRegistry::ForEachActiveSnapshot(const NKikimr::TTableId& tableId,
    const TRowVersion& from, const TRowVersion& to,
    TFunctionRef<void(const TRowVersion&)> visitor) const
{
    if (from >= to) {
        return;
    }
    const auto snapshots = GetActiveSnapshots(tableId);
    for (auto it = snapshots.lower_bound(from); it != snapshots.end() && *it < to; ++it) {
        visitor(*it);
    }
}

IImmutableSnapshotRegistryHolderPtr CreateImmutableSnapshotRegistryHolder() {
    return MakeIntrusive<TImmutableSnapshotRegistryHolder>();
}

TIntrusivePtr<IImmutableSnapshotRegistryBuilder> CreateImmutableSnapshotRegistryBuilder() {
    return MakeIntrusive<TImmutableSnapshotRegistryBuilder>();
}

} // namespace NKikimr
