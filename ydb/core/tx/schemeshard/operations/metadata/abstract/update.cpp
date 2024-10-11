#include "update.h"

#include <ydb/core/tx/schemeshard/operations/metadata/tiering_rule/update.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>
#include <ydb/core/tx/tiering/rule/object.h>

namespace NKikimr::NSchemeShard::NOperations {

std::shared_ptr<IDropMetadataUpdate> TMetadataUpdate::MakeDrop(const TPath& object) {
    switch (object->PathType) {
        case NKikimrSchemeOp::EPathTypeTieringRule:
            return std::make_shared<TDropTieringRule>(object.PathString());
        default:
            return nullptr;
    }
}

std::shared_ptr<TMetadataUpdate> TMetadataUpdate::MakeUpdate(const NKikimrSchemeOp::TModifyScheme& transaction) {
    switch (transaction.GetOperationType()) {
        case NKikimrSchemeOp::ESchemeOpCreateTieringRule:
            return std::make_shared<TCreateTieringRule>(JoinPath({ transaction.GetWorkingDir(), transaction.GetCreateTieringRule().GetName() }));
        case NKikimrSchemeOp::ESchemeOpAlterTieringRule:
            return std::make_shared<TAlterTieringRule>(JoinPath({ transaction.GetWorkingDir(), transaction.GetCreateTieringRule().GetName() }));
        case NKikimrSchemeOp::ESchemeOpDropTieringRule:
            if (!transaction.GetDrop().HasName()) {
                return nullptr;
            }
            return std::make_shared<TDropTieringRule>(JoinPath({ transaction.GetWorkingDir(), transaction.GetDrop().GetName() }));
        default:
            return nullptr;
    }
}

std::shared_ptr<IDropMetadataUpdate> TMetadataUpdate::RestoreDrop(const IDropMetadataUpdate::TRestoreContext& context) {
    std::shared_ptr<IDropMetadataUpdate> update = MakeDrop(*context.GetObjectPath());
    update->RestoreDrop(context);
    return update;
}

}   // namespace NKikimr::NSchemeShard::NOperations
