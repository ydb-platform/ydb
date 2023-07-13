#include "pdisks.h"
#include "base.h"

namespace NKikimr::NSysView {

template<> void SetField<0>(NKikimrSysView::TPDiskKey& key, ui32 value) { key.SetNodeId(value); }
template<> void SetField<1>(NKikimrSysView::TPDiskKey& key, ui32 value) { key.SetPDiskId(value); }

class TPDisksScan : public TStorageScanBase<TPDisksScan, TEvSysView::TEvGetPDisksResponse> {
public:
    using TStorageScanBase::TStorageScanBase;

    static constexpr const char *GetName() { return "TPDisksScan"; }

    TEvSysView::TEvGetPDisksRequest *CreateQuery() {
        auto request = MakeHolder<TEvSysView::TEvGetPDisksRequest>();
        ConvertKeyRange<NKikimrSysView::TEvGetPDisksRequest, ui32, ui32>(request->Record, TableRange);
        return request.Release();
    }

    static const TFieldMap& GetFieldMap() {
        using T = Schema::PDisks;
        using E = NKikimrSysView::TPDiskEntry;
        using K = NKikimrSysView::TPDiskKey;
        using V = NKikimrSysView::TPDiskInfo;
        static TFieldMap fieldMap{
            {T::NodeId::ColumnId, {E::kKeyFieldNumber, K::kNodeIdFieldNumber}},
            {T::PDiskId::ColumnId, {E::kKeyFieldNumber, K::kPDiskIdFieldNumber}},
            {T::TypeCol::ColumnId, {E::kInfoFieldNumber, V::kTypeFieldNumber}},
            {T::Kind::ColumnId, {E::kInfoFieldNumber, V::kKindFieldNumber}},
            {T::Path::ColumnId, {E::kInfoFieldNumber, V::kPathFieldNumber}},
            {T::Guid::ColumnId, {E::kInfoFieldNumber, V::kGuidFieldNumber}},
            {T::BoxId::ColumnId, {E::kInfoFieldNumber, V::kBoxIdFieldNumber}},
            {T::SharedWithOS::ColumnId, {E::kInfoFieldNumber, V::kSharedWithOsFieldNumber}},
            {T::ReadCentric::ColumnId, {E::kInfoFieldNumber, V::kReadCentricFieldNumber}},
            {T::AvailableSize::ColumnId, {E::kInfoFieldNumber, V::kAvailableSizeFieldNumber}},
            {T::TotalSize::ColumnId, {E::kInfoFieldNumber, V::kTotalSizeFieldNumber}},
            {T::Status::ColumnId, {E::kInfoFieldNumber, V::kStatusV2FieldNumber}},
            {T::StatusChangeTimestamp::ColumnId, {E::kInfoFieldNumber, V::kStatusChangeTimestampFieldNumber}},
            {T::ExpectedSlotCount::ColumnId, {E::kInfoFieldNumber, V::kExpectedSlotCountFieldNumber}},
            {T::NumActiveSlots::ColumnId, {E::kInfoFieldNumber, V::kNumActiveSlotsFieldNumber}},
            {T::DecommitStatus::ColumnId, {E::kInfoFieldNumber, V::kDecommitStatusFieldNumber}},
        };
        return fieldMap;
    }
};

THolder<NActors::IActor> CreatePDisksScan(const NActors::TActorId& ownerId, ui32 scanId, const TTableId& tableId,
    const TTableRange& tableRange, const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns)
{
    return MakeHolder<TPDisksScan>(ownerId, scanId, tableId, tableRange, columns);
}

} // NKikimr::NSysView
