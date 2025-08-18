#include "groups.h"
#include "base.h"

namespace NKikimr::NSysView {

template<> void SetField<0>(NKikimrSysView::TGroupKey& key, ui32 value) { key.SetGroupId(value); }

class TGroupsScan : public TStorageScanBase<TGroupsScan, TEvSysView::TEvGetGroupsResponse> {
public:
    using TStorageScanBase::TStorageScanBase;

    static constexpr const char *GetName() { return "TGroupsScan"; }

    TEvSysView::TEvGetGroupsRequest *CreateQuery() {
        auto request = MakeHolder<TEvSysView::TEvGetGroupsRequest>();
        ConvertKeyRange<NKikimrSysView::TEvGetGroupsRequest, ui32>(request->Record, TableRange);
        return request.Release();
    }

    static const TFieldMap& GetFieldMap() {
        using T = Schema::Groups;
        using E = NKikimrSysView::TGroupEntry;
        using K = NKikimrSysView::TGroupKey;
        using V = NKikimrSysView::TGroupInfo;
        static TFieldMap fieldMap{
            {T::GroupId::ColumnId, {E::kKeyFieldNumber, K::kGroupIdFieldNumber}},
            {T::Generation::ColumnId, {E::kInfoFieldNumber, V::kGenerationFieldNumber}},
            {T::ErasureSpecies::ColumnId, {E::kInfoFieldNumber, V::kErasureSpeciesV2FieldNumber}},
            {T::BoxId::ColumnId, {E::kInfoFieldNumber, V::kBoxIdFieldNumber}},
            {T::StoragePoolId::ColumnId, {E::kInfoFieldNumber, V::kStoragePoolIdFieldNumber}},
            {T::EncryptionMode::ColumnId, {E::kInfoFieldNumber, V::kEncryptionModeFieldNumber}},
            {T::LifeCyclePhase::ColumnId, {E::kInfoFieldNumber, V::kLifeCyclePhaseFieldNumber}},
            {T::AllocatedSize::ColumnId, {E::kInfoFieldNumber, V::kAllocatedSizeFieldNumber}},
            {T::AvailableSize::ColumnId, {E::kInfoFieldNumber, V::kAvailableSizeFieldNumber}},
            {T::SeenOperational::ColumnId, {E::kInfoFieldNumber, V::kSeenOperationalFieldNumber}},
            {T::PutTabletLogLatency::ColumnId, {E::kInfoFieldNumber, V::kPutTabletLogLatencyFieldNumber}},
            {T::PutUserDataLatency::ColumnId, {E::kInfoFieldNumber, V::kPutUserDataLatencyFieldNumber}},
            {T::GetFastLatency::ColumnId, {E::kInfoFieldNumber, V::kGetFastLatencyFieldNumber}},
            {T::LayoutCorrect::ColumnId, {E::kInfoFieldNumber, V::kLayoutCorrectFieldNumber}},
            {T::OperatingStatus::ColumnId, {E::kInfoFieldNumber, V::kOperatingStatusFieldNumber}},
            {T::ExpectedStatus::ColumnId, {E::kInfoFieldNumber, V::kExpectedStatusFieldNumber}},
            {T::ProxyGroupId::ColumnId, {E::kInfoFieldNumber, V::kProxyGroupIdFieldNumber}},
            {T::BridgePileId::ColumnId, {E::kInfoFieldNumber, V::kBridgePileIdFieldNumber}},
            {T::GroupSizeInUnits::ColumnId, {E::kInfoFieldNumber, V::kGroupSizeInUnitsFieldNumber}},
        };
        return fieldMap;
    }

};
THolder<NActors::IActor> CreateGroupsScan(const NActors::TActorId& ownerId, ui32 scanId,
    const NKikimrSysView::TSysViewDescription& sysViewInfo, const TTableRange& tableRange,
    const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns)
{
    return MakeHolder<TGroupsScan>(ownerId, scanId, sysViewInfo, tableRange, columns);
}

} // NKikimr::NSysView
