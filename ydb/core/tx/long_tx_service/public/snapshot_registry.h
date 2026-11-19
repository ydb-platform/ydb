#pragma once

#include <ydb/core/base/row_version.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <util/system/types.h>
#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/set.h>
#include <util/generic/vector.h>
#include <util/generic/intrlist.h>
#include <util/generic/function_ref.h>
#include <library/cpp/threading/atomic_shared_ptr/atomic_shared_ptr.h>

namespace NKikimr {

class IImmutableSnapshotRegistry {
public:
    virtual ~IImmutableSnapshotRegistry() = default;

    virtual bool HasSnapshot(const NKikimr::TTableId& tableId, const TRowVersion& version) const = 0;
    virtual TSet<TRowVersion> GetActiveSnapshots(const NKikimr::TTableId& tableId) const = 0;

    // Visit explicit table-specific and all-table registrations in [from, to),
    // in ascending order without duplicates. References are valid during the
    // callback. The production registry overrides this to avoid materializing
    // a set; the default preserves compatibility with existing implementations.
    virtual void ForEachActiveSnapshot(const NKikimr::TTableId& tableId,
        const TRowVersion& from, const TRowVersion& to,
        TFunctionRef<void(const TRowVersion&)> visitor) const;

    virtual TRowVersion GetBorder() const = 0;

    // Freshness bound for cleanup: oldest collection time across all nodes (local node = now).
    virtual TInstant GetOldestCollectionTime() const = 0;
};

class IImmutableSnapshotRegistryHolder : public TThrRefBase {
public:
   virtual ~IImmutableSnapshotRegistryHolder() = default;
   
   virtual const TTrueAtomicSharedPtr<IImmutableSnapshotRegistry>& Get() const = 0;

   virtual void Set(std::unique_ptr<IImmutableSnapshotRegistry>&& registry) = 0;
};

using IImmutableSnapshotRegistryHolderPtr = TIntrusivePtr<IImmutableSnapshotRegistryHolder>;

IImmutableSnapshotRegistryHolderPtr CreateImmutableSnapshotRegistryHolder();

class IImmutableSnapshotRegistryBuilder : public TThrRefBase {
public:
    virtual ~IImmutableSnapshotRegistryBuilder() = default;

    virtual void SetSnapshotBorder(const TRowVersion& version) = 0;

    virtual void SetOldestCollectionTime(TInstant oldestCollectionTime) = 0;

    virtual void AddSnapshot(const TVector<NKikimr::TTableId>& tableIds, const TRowVersion& version) = 0;

    virtual std::unique_ptr<IImmutableSnapshotRegistry> Build() && = 0;
};

using IImmutableSnapshotRegistryBuilderPtr = TIntrusivePtr<IImmutableSnapshotRegistryBuilder>;

IImmutableSnapshotRegistryBuilderPtr CreateImmutableSnapshotRegistryBuilder();

} // namespace NKikimr
