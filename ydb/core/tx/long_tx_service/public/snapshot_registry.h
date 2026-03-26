#pragma once

#include <ydb/core/base/row_version.h>
#include <util/system/types.h>
#include <util/generic/hash.h>
#include <util/generic/vector.h>
#include <util/generic/intrlist.h>
#include <library/cpp/threading/atomic_shared_ptr/atomic_shared_ptr.h>

namespace NKikimr {

class IImmutableSnapshotRegistry {
public:
    virtual ~IImmutableSnapshotRegistry() = default;

    virtual bool QuerySnapshots(
        ui64 tableId,
        const TRowVersion& begin,
        const TRowVersion& end) const = 0;

    virtual bool HasSnapshot(ui64 tableId, const TRowVersion& version) const = 0;
};

class IImmutableSnapshotRegistryHolder : public TThrRefBase {
public:
   virtual ~IImmutableSnapshotRegistryHolder() = default;
   
   virtual const TTrueAtomicSharedPtr<IImmutableSnapshotRegistry>& Get() const = 0;

   virtual void Set(IImmutableSnapshotRegistry* registry) = 0;
};

using IImmutableSnapshotRegistryHolderPtr = TIntrusivePtr<IImmutableSnapshotRegistryHolder>;

IImmutableSnapshotRegistryHolderPtr CreateImmutableSnapshotRegistryHolder();

class IImmutableSnapshotRegistryBuilder : public TThrRefBase {
public:
    virtual ~IImmutableSnapshotRegistryBuilder() = default;

    virtual void SetSnapshotBorder(const TRowVersion& version) = 0;

    virtual void AddSnapshot(const TVector<ui64>& tableId, const TRowVersion& version) = 0;

    virtual std::unique_ptr<IImmutableSnapshotRegistry> Build() && = 0;
};

using IImmutableSnapshotRegistryBuilderPtr = TIntrusivePtr<IImmutableSnapshotRegistryBuilder>;

IImmutableSnapshotRegistryBuilderPtr CreateImmutableSnapshotRegistryBuilder();

} // namespace NKikimr