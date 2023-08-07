#include "storage_stats.h"
#include "base.h"

namespace NKikimr::NSysView {

class TStorageStatsScan : public TStorageScanBase<TStorageStatsScan, TEvSysView::TEvGetStorageStatsResponse> {
public:
    using TStorageScanBase::TStorageScanBase;

    static constexpr const char *GetName() { return "TStorageStatsScan"; }

    TEvSysView::TEvGetStorageStatsRequest *CreateQuery() {
        return new TEvSysView::TEvGetStorageStatsRequest();
    }

    static const TFieldMap& GetFieldMap() {
        using T = Schema::StorageStats;
        using E = NKikimrSysView::TStorageStatsEntry;
        static TFieldMap fieldMap{
            {T::PDiskFilter::ColumnId, {E::kPDiskFilterFieldNumber}},
            {T::ErasureSpecies::ColumnId, {E::kErasureSpeciesFieldNumber}},
            {T::CurrentGroupsCreated::ColumnId, {E::kCurrentGroupsCreatedFieldNumber}},
            {T::CurrentAllocatedSize::ColumnId, {E::kCurrentAllocatedSizeFieldNumber}},
            {T::CurrentAvailableSize::ColumnId, {E::kCurrentAvailableSizeFieldNumber}},
            {T::AvailableGroupsToCreate::ColumnId, {E::kAvailableGroupsToCreateFieldNumber}},
            {T::AvailableSizeToCreate::ColumnId, {E::kAvailableSizeToCreateFieldNumber}},
        };
        return fieldMap;
    }
};

THolder<NActors::IActor> CreateStorageStatsScan(const NActors::TActorId& ownerId, ui32 scanId, const TTableId& tableId,
    const TTableRange& tableRange, const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns)
{
    return MakeHolder<TStorageStatsScan>(ownerId, scanId, tableId, tableRange, columns);
}

} // NKikimr::NSysView
