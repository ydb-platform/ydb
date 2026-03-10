#include "snapshot_registry.h"

#include <ydb/library/actors/core/log.h>

namespace NKikimr {

namespace {
    constexpr ui64 kKeyAllTables = 0;

    class TImmutableSnapshotRegistry : public IImmutableSnapshotRegistry {
    public:
        using TSnapshotMap = THashMap<ui64, TVector<TRowVersion>>;

        TImmutableSnapshotRegistry() = default;
        
        explicit TImmutableSnapshotRegistry(TSnapshotMap&& snapshots, TRowVersion snapshotBorder)
            : Snapshots(std::move(snapshots))
            , SnapshotBorder(snapshotBorder)
        {}

        ~TImmutableSnapshotRegistry() override = default;
        
        bool QuerySnapshots(
            ui64 tableId,
            const TRowVersion& begin,
            const TRowVersion& end) const override
        {
            AFL_ENSURE(tableId != kKeyAllTables);
            return QuerySnapshotsImpl(tableId, begin, end) || QuerySnapshotsImpl(kKeyAllTables, begin, end);
        }
        
        bool HasSnapshot(ui64 tableId, const TRowVersion& version) const override {
            AFL_ENSURE(tableId != kKeyAllTables);
            if (version >= SnapshotBorder) {
                return true;
            }
            return HasSnapshotImpl(tableId, version) || HasSnapshotImpl(kKeyAllTables, version);
        }
        
    private:
        bool QuerySnapshotsImpl(
            ui64 tableId,
            const TRowVersion& begin,
            const TRowVersion& end) const
        {
            if (begin >= end) {
                return false;
            }
            if (begin >= SnapshotBorder) {
                return true;
            }

            auto snapshotsIter = Snapshots.find(tableId);
            if (snapshotsIter == Snapshots.end()) {
                return false;
            }
            
            const auto& versions = snapshotsIter->second;        
            const auto versionsIter = std::lower_bound(versions.begin(), versions.end(), begin);
            return versionsIter != versions.end() && begin <= *versionsIter && *versionsIter < end;
        }
        
        bool HasSnapshotImpl(ui64 tableId, const TRowVersion& version) const {
            auto snapshotsIter = Snapshots.find(tableId);
            if (snapshotsIter == Snapshots.end()) {
                return false;
            }
            
            const auto& versions = snapshotsIter->second;

            auto versionsIter = std::lower_bound(versions.begin(), versions.end(), version);
            return versionsIter != versions.end() && *versionsIter == version;
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

        void Set(IImmutableSnapshotRegistry* registry) override {
            TTrueAtomicSharedPtr<IImmutableSnapshotRegistry> newRegistry(registry);
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
        
        void AddSnapshot(const TVector<ui64>& tableIds, const TRowVersion& version) override {
            if (version >= SnapshotBorder) {
                return;
            }

            if (tableIds.empty()) {
                Snapshots[kKeyAllTables].push_back(version);
            } else {
                for (ui64 tableId : tableIds) {
                    Snapshots[tableId].push_back(version);
                }
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
        THashMap<ui64, TVector<TRowVersion>> Snapshots;
    };
}

IImmutableSnapshotRegistryHolderPtr CreateImmutableSnapshotRegistryHolder() {
    return MakeIntrusive<TImmutableSnapshotRegistryHolder>();
}

TIntrusivePtr<IImmutableSnapshotRegistryBuilder> CreateImmutableSnapshotRegistryBuilder() {
    return MakeIntrusive<TImmutableSnapshotRegistryBuilder>();
}

} // namespace NKikimr
