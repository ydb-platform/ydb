#include "object.h"
#include "update.h"
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>
#include <ydb/core/tx/schemeshard/olap/operations/alter/protos/evolution.pb.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

NKikimr::TConclusionStatus ISSEntity::StartUpdate(const TUpdateInitializationContext& uContext, const TStartUpdateContext& sContext, const std::shared_ptr<ISSEntity>& selfPtr) {
    std::shared_ptr<ISSEntityUpdate> update;
    {
        auto conclusion = CreateUpdate(uContext, selfPtr);
        if (conclusion.IsFail()) {
            return conclusion;
        }
        update = conclusion.DetachResult();
    }
    {
        auto conclusion = update->BuildEvolutions();
        if (conclusion.IsFail()) {
            return conclusion;
        }
        return DoStartUpdate(conclusion.DetachResult(), sContext);
    }
}

std::shared_ptr<NKikimr::NSchemeShard::NOlap::NAlter::ISSEntity> ISSEntity::GetPathEntityVerified(TOperationContext& context, const TPath& path) {
    if (path->IsColumnTable()) {
        auto readGuard = context.SS->ColumnTables.GetVerified(path.GetPathIdForDomain());
        TEntityInitializationContext iContext(&context);
        return readGuard->BuildEntity(path.GetPathIdForDomain(), iContext).DetachResult();
    }
    AFL_VERIFY(false);
    return nullptr;
}

TVector<NKikimr::NSchemeShard::ISubOperation::TPtr> ISSEntity::BuildOperations(const TUpdateInitializationContext& context,
    const std::shared_ptr<ISSEntity>& selfPtr, const TOperationId& id) const {
    AFL_VERIFY(Initialized);
    return CreateUpdate(context, selfPtr).DetachResult()->BuildOperations(id, *context.GetModification());
}

NKikimrSchemeshardOlap::TEvolutions TEvolutions::SerializeToProto() const {
    NKikimrSchemeshardOlap::TEvolutions result;
    for (auto&& i : Evolutions) {
        *result.AddEvolutions() = i.SerializeToProto();
    }
    return result;
}

NKikimr::TConclusionStatus TEvolutions::DeserializeFromProto(const TString& protoStr) {
    NKikimrSchemeshardOlap::TEvolutions evolutionsProto;
    if (!evolutionsProto.ParseFromString(protoStr)) {
        return TConclusionStatus::Fail("cannot parse proto");
    }
    return DeserializeFromProto(evolutionsProto);
}

NKikimr::TConclusionStatus TEvolutions::DeserializeFromProto(const NKikimrSchemeshardOlap::TEvolutions& proto) {
    if (!proto.HasAlterProtoOriginal()) {
        return TConclusionStatus::Fail("has no data about original request proto");
    }
    AlterProtoOriginal = proto.GetAlterProtoOriginal();
    for (auto&& i : proto.GetEvolutions()) {
        TSSEntityEvolutionContainer evolution;
        if (!evolution.DeserializeFromProto(i)) {
            return TConclusionStatus::Fail("cannot parse evolution: " + i.DebugString());
        }
    }
    return TConclusionStatus::Success();
}

TString TEvolutions::SerializeToProtoString() const {
    return SerializeToProto().SerializeAsString();
}

}