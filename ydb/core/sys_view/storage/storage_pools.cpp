#include "storage_pools.h"
#include "base.h"

namespace NKikimr::NSysView {

template<> void SetField<0>(NKikimrSysView::TStoragePoolKey& key, ui64 value) { key.SetBoxId(value); }
template<> void SetField<1>(NKikimrSysView::TStoragePoolKey& key, ui64 value) { key.SetStoragePoolId(value); }

class TStoragePoolsScan : public TStorageScanBase<TStoragePoolsScan, TEvSysView::TEvGetStoragePoolsResponse> {
public:
    using TStorageScanBase::TStorageScanBase;

    static constexpr const char *GetName() { return "TStoragePoolsScan"; }

    TEvSysView::TEvGetStoragePoolsRequest *CreateQuery() {
        auto request = MakeHolder<TEvSysView::TEvGetStoragePoolsRequest>();
        ConvertKeyRange<NKikimrSysView::TEvGetStoragePoolsRequest, ui64, ui64>(request->Record, TableRange);
        return request.Release();
    }

    static const TFieldMap& GetFieldMap() {
        using T = Schema::StoragePools;
        using E = NKikimrSysView::TStoragePoolEntry;
        using K = NKikimrSysView::TStoragePoolKey;
        using V = NKikimrSysView::TStoragePoolInfo;
        static TFieldMap fieldMap{
            {T::BoxId::ColumnId, {E::kKeyFieldNumber, K::kBoxIdFieldNumber}},
            {T::StoragePoolId::ColumnId, {E::kKeyFieldNumber, K::kStoragePoolIdFieldNumber}},
            {T::Name::ColumnId, {E::kInfoFieldNumber, V::kNameFieldNumber}},
            {T::Generation::ColumnId, {E::kInfoFieldNumber, V::kGenerationFieldNumber}},
            {T::ErasureSpecies::ColumnId, {E::kInfoFieldNumber, V::kErasureSpeciesV2FieldNumber}},
            {T::VDiskKind::ColumnId, {E::kInfoFieldNumber, V::kVDiskKindV2FieldNumber}},
            {T::Kind::ColumnId, {E::kInfoFieldNumber, V::kKindFieldNumber}},
            {T::NumGroups::ColumnId, {E::kInfoFieldNumber, V::kNumGroupsFieldNumber}},
            {T::EncryptionMode::ColumnId, {E::kInfoFieldNumber, V::kEncryptionModeFieldNumber}},
            {T::SchemeshardId::ColumnId, {E::kInfoFieldNumber, V::kSchemeshardIdFieldNumber}},
            {T::PathId::ColumnId, {E::kInfoFieldNumber, V::kPathIdFieldNumber}},
        };
        return fieldMap;
    }
};

THolder<NActors::IActor> CreateStoragePoolsScan(const NActors::TActorId& ownerId, ui32 scanId, const TTableId& tableId,
    const TTableRange& tableRange, const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns)
{
    return MakeHolder<TStoragePoolsScan>(ownerId, scanId, tableId, tableRange, columns);
}

} // NKikimr::NSysView
