#include "kqp_locks_helper.h"

#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/utils/yql_panic.h>

#include <ydb/core/protos/data_events.pb.h>


namespace NKikimr::NKqp {

void BuildLocks(NKikimrMiniKQL::TResult& result, const TVector<NKikimrDataEvents::TLock>& locks) {
    auto setMemberDataType = [] (NKikimrMiniKQL::TMember& member, const TString& name, ui32 scheme) {
        member.SetName(name);
        member.MutableType()->SetKind(NKikimrMiniKQL::ETypeKind::Data);
        member.MutableType()->MutableData()->SetScheme(scheme);
    };

    auto& type = *result.MutableType();
    type.SetKind(NKikimrMiniKQL::ETypeKind::List);
    auto& itemType = *type.MutableList()->MutableItem();
    itemType.SetKind(NKikimrMiniKQL::ETypeKind::Struct);
    auto& structType = *itemType.MutableStruct();
    setMemberDataType(*structType.AddMember(), "Counter", NKikimr::NUdf::TDataType<ui64>::Id);
    setMemberDataType(*structType.AddMember(), "DataShard", NKikimr::NUdf::TDataType<ui64>::Id);
    setMemberDataType(*structType.AddMember(), "Generation", NKikimr::NUdf::TDataType<ui32>::Id);
    setMemberDataType(*structType.AddMember(), "LockId", NKikimr::NUdf::TDataType<ui64>::Id);
    setMemberDataType(*structType.AddMember(), "PathId", NKikimr::NUdf::TDataType<ui64>::Id);
    setMemberDataType(*structType.AddMember(), "SchemeShard", NKikimr::NUdf::TDataType<ui64>::Id);
    setMemberDataType(*structType.AddMember(), "HasWrites", NKikimr::NUdf::TDataType<bool>::Id);

    auto& value = *result.MutableValue();
    for (auto& lock : locks) {
        auto& item = *value.AddList();
        item.AddStruct()->SetUint64(lock.GetCounter());
        item.AddStruct()->SetUint64(lock.GetDataShard());
        item.AddStruct()->SetUint32(lock.GetGeneration());
        item.AddStruct()->SetUint64(lock.GetLockId());
        item.AddStruct()->SetUint64(lock.GetPathId());
        item.AddStruct()->SetUint64(lock.GetSchemeShard());
        item.AddStruct()->SetBool(lock.GetHasWrites());
    }
}

NKikimrDataEvents::TLock ExtractLock(const NYql::NDq::TMkqlValueRef& lock) {
    auto ensureMemberDataType = [] (const NKikimrMiniKQL::TMember& member, const TString& name, ui32 scheme) {
        YQL_ENSURE(member.GetName() == name);
        YQL_ENSURE(member.GetType().GetKind() == NKikimrMiniKQL::ETypeKind::Data);
        YQL_ENSURE(member.GetType().GetData().GetScheme() == scheme);
    };

    const auto& type = lock.GetType();
    const auto& value = lock.GetValue();

    YQL_ENSURE(type.GetKind() == NKikimrMiniKQL::ETypeKind::Struct);
    auto& structType = type.GetStruct();

    YQL_ENSURE(structType.MemberSize() == 7);
    ensureMemberDataType(structType.GetMember(0), "Counter", NKikimr::NUdf::TDataType<ui64>::Id);
    ensureMemberDataType(structType.GetMember(1), "DataShard", NKikimr::NUdf::TDataType<ui64>::Id);
    ensureMemberDataType(structType.GetMember(2), "Generation", NKikimr::NUdf::TDataType<ui32>::Id);
    ensureMemberDataType(structType.GetMember(3), "LockId", NKikimr::NUdf::TDataType<ui64>::Id);
    ensureMemberDataType(structType.GetMember(4), "PathId", NKikimr::NUdf::TDataType<ui64>::Id);
    ensureMemberDataType(structType.GetMember(5), "SchemeShard", NKikimr::NUdf::TDataType<ui64>::Id);
    ensureMemberDataType(structType.GetMember(6), "HasWrites", NKikimr::NUdf::TDataType<bool>::Id);

    NKikimrDataEvents::TLock dsLock;
    dsLock.SetCounter(value.GetStruct(0).GetUint64());
    dsLock.SetDataShard(value.GetStruct(1).GetUint64());
    dsLock.SetGeneration(value.GetStruct(2).GetUint32());
    dsLock.SetLockId(value.GetStruct(3).GetUint64());
    dsLock.SetPathId(value.GetStruct(4).GetUint64());
    dsLock.SetSchemeShard(value.GetStruct(5).GetUint64());
    dsLock.SetHasWrites(value.GetStruct(6).GetBool());

    return dsLock;
}

} // namespace NKikimr::NKqp
