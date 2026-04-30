#include "snapshot_registry.h"

#include <ydb/library/actors/core/log.h>

namespace NKikimr {

namespace {

    class TImmutableSnapshotRegistry : public IImmutableSnapshotRegistry {
    public:
        using TSnapshotMap = THashMap<NKikimr::TTableId, TVector<TRowVersion>>;

        TImmutableSnapshotRegistry() = default;
        
        explicit TImmutableSnapshotRegistry(TSnapshotMap&& snapshots, TRowVersion snapshotBorder)
            : Snapshots(std::move(snapshots))
            , SnapshotBorder(snapshotBorder)
        {}

        ~TImmutableSnapshotRegistry() override = default;
        
        bool HasSnapshot(const NKikimr::TTableId& tableId, const TRowVersion& version) const override {
            if (version >= SnapshotBorder) {
                return true;
            }
            return HasSnapshotImpl(tableId, version) || HasSnapshotImpl(NKikimr::TTableId{}, version);
        }

        TSet<TRowVersion> GetActiveSnapshots(const NKikimr::TTableId& tableId) const override {
            TSet<TRowVersion> result;
            AddSnapshotsImpl(tableId, result);
            AddSnapshotsImpl(NKikimr::TTableId{}, result);
            return result;
        }

        TRowVersion GetBorder() const override {
            return SnapshotBorder;
        }
        
    private:        
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
    };

    class TImmutableSnapshotRegistryHolder : public IImmutableSnapshotRegistryHolder {
    public:
        TImmutableSnapshotRegistryHolder() = default;

        ~TImmutableSnapshotRegistryHolder() override = default;

        const TTrueAtomicSharedPtr<IImmutableSnapshotRegistry>& Get() const override {
            return Registry;
        }

        void Set(std::unique_ptr<IImmutableSnapshotRegistry>&& registry) override {
            TTrueAtomicSharedPtr<IImmutableSnapshotRegistry> newRegistry(registry.release());
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
        
        void AddSnapshot(const TVector<NKikimr::TTableId>& tableIds, const TRowVersion& version) override {
            if (version >= SnapshotBorder) {
                return;
            }

            if (tableIds.empty()) {
                Snapshots[NKikimr::TTableId{}].push_back(version);
            }

            for (const NKikimr::TTableId& tableId : tableIds) {
                Snapshots[tableId].push_back(version);
            }
        }
        
        std::unique_ptr<IImmutableSnapshotRegistry> Build() && override {
            for (auto& [tableId, versions] : Snapshots) {
                std::sort(versions.begin(), versions.end());
            }
            return std::make_unique<TImmutableSnapshotRegistry>(std::move(Snapshots), SnapshotBorder);
        }

    private:
        TRowVersion SnapshotBorder = TRowVersion::Max();
        THashMap<NKikimr::TTableId, TVector<TRowVersion>> Snapshots;
    };
}

IImmutableSnapshotRegistryHolderPtr CreateImmutableSnapshotRegistryHolder() {
    return MakeIntrusive<TImmutableSnapshotRegistryHolder>();
}

TIntrusivePtr<IImmutableSnapshotRegistryBuilder> CreateImmutableSnapshotRegistryBuilder() {
    return MakeIntrusive<TImmutableSnapshotRegistryBuilder>();
}

} // namespace NKikimr
