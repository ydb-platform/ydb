#include "vslots.h"
#include "base.h"

namespace NKikimr::NSysView {

template<> void SetField<0>(NKikimrSysView::TVSlotKey& key, ui32 value) { key.SetNodeId(value); }
template<> void SetField<1>(NKikimrSysView::TVSlotKey& key, ui32 value) { key.SetPDiskId(value); }
template<> void SetField<2>(NKikimrSysView::TVSlotKey& key, ui32 value) { key.SetVSlotId(value); }

class TVSlotsScan : public TStorageScanBase<TVSlotsScan, TEvSysView::TEvGetVSlotsResponse> {
public:
    using TStorageScanBase::TStorageScanBase;

    static constexpr const char *GetName() { return "TVSlotsScan"; }

    TEvSysView::TEvGetVSlotsRequest *CreateQuery() {
        auto request = MakeHolder<TEvSysView::TEvGetVSlotsRequest>();
        ConvertKeyRange<NKikimrSysView::TEvGetVSlotsRequest, ui32, ui32, ui32>(request->Record, TableRange);
        return request.Release();
    }

    static const TFieldMap& GetFieldMap() {
        using T = Schema::VSlots;
        using E = NKikimrSysView::TVSlotEntry;
        using K = NKikimrSysView::TVSlotKey;
        using V = NKikimrSysView::TVSlotInfo;
        static TFieldMap fieldMap{
            {T::NodeId::ColumnId, {E::kKeyFieldNumber, K::kNodeIdFieldNumber}},
            {T::PDiskId::ColumnId, {E::kKeyFieldNumber, K::kPDiskIdFieldNumber}},
            {T::VSlotId::ColumnId, {E::kKeyFieldNumber, K::kVSlotIdFieldNumber}},
            {T::GroupId::ColumnId, {E::kInfoFieldNumber, V::kGroupIdFieldNumber}},
            {T::GroupGeneration::ColumnId, {E::kInfoFieldNumber, V::kGroupGenerationFieldNumber}},
            {T::FailRealm::ColumnId, {E::kInfoFieldNumber, V::kFailRealmFieldNumber}},
            {T::FailDomain::ColumnId, {E::kInfoFieldNumber, V::kFailDomainFieldNumber}},
            {T::VDisk::ColumnId, {E::kInfoFieldNumber, V::kVDiskFieldNumber}},
            {T::AllocatedSize::ColumnId, {E::kInfoFieldNumber, V::kAllocatedSizeFieldNumber}},
            {T::AvailableSize::ColumnId, {E::kInfoFieldNumber, V::kAvailableSizeFieldNumber}},
            {T::Status::ColumnId, {E::kInfoFieldNumber, V::kStatusV2FieldNumber}},
            {T::State::ColumnId, {E::kInfoFieldNumber, V::kStateFieldNumber}},
            {T::Kind::ColumnId, {E::kInfoFieldNumber, V::kKindFieldNumber}},
            {T::Replicated::ColumnId, {E::kInfoFieldNumber, V::kReplicatedFieldNumber}},
            {T::DiskSpace::ColumnId, {E::kInfoFieldNumber, V::kDiskSpaceFieldNumber}},
        };
        return fieldMap;
    }
};

THolder<NActors::IActor> CreateVSlotsScan(const NActors::TActorId& ownerId, ui32 scanId,
    const NKikimrSysView::TSysViewDescription& sysViewInfo, const TTableRange& tableRange,
    const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns)
{
    return MakeHolder<TVSlotsScan>(ownerId, scanId, sysViewInfo, tableRange, columns);
}

} // NKikimr::NSysView
