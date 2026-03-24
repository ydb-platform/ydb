#include "schemeshard_impl.h"
#include "schemeshard_set_column_constraint.h"

namespace NKikimr {
namespace NSchemeShard {

void TSchemeShard::PersistCreateSetColumnConstraint([[maybe_unused]] NIceDb::TNiceDb& db, [[maybe_unused]] const TSetColumnConstraintOperationInfo& operationInfo) {
    // todo
}

void TSchemeShard::PersistSetColumnConstraintState([[maybe_unused]] NIceDb::TNiceDb& db, [[maybe_unused]] const TSetColumnConstraintOperationInfo& operationInfo) {
    // todo
}

void TSchemeShard::PersistSetColumnConstraintLockTxId([[maybe_unused]] NIceDb::TNiceDb& db, [[maybe_unused]] const TSetColumnConstraintOperationInfo& operationInfo) {
    // todo
}

void TSchemeShard::PersistSetColumnConstraintLockTxStatus([[maybe_unused]] NIceDb::TNiceDb& db, [[maybe_unused]] const TSetColumnConstraintOperationInfo& operationInfo) {
    // todo
}

void TSchemeShard::PersistSetColumnConstraintLockTxDone([[maybe_unused]] NIceDb::TNiceDb& db, [[maybe_unused]] const TSetColumnConstraintOperationInfo& operationInfo) {
    // todo
}

void TSchemeShard::PersistSetColumnConstraintLockNullWritesTxId([[maybe_unused]] NIceDb::TNiceDb& db, [[maybe_unused]] const TSetColumnConstraintOperationInfo& operationInfo) {
    // todo
}

void TSchemeShard::PersistSetColumnConstraintLockNullWritesTxStatus([[maybe_unused]] NIceDb::TNiceDb& db, [[maybe_unused]] const TSetColumnConstraintOperationInfo& operationInfo) {
    // todo
}

void TSchemeShard::PersistSetColumnConstraintLockNullWritesTxDone([[maybe_unused]] NIceDb::TNiceDb& db, [[maybe_unused]] const TSetColumnConstraintOperationInfo& operationInfo) {
    // todo
}

void TSchemeShard::PersistSetColumnConstraintUnlockNullWritesTxId([[maybe_unused]] NIceDb::TNiceDb& db, [[maybe_unused]] const TSetColumnConstraintOperationInfo& operationInfo) {
    // todo
}

void TSchemeShard::PersistSetColumnConstraintUnlockNullWritesTxStatus([[maybe_unused]] NIceDb::TNiceDb& db, [[maybe_unused]] const TSetColumnConstraintOperationInfo& operationInfo) {
    // todo
}

void TSchemeShard::PersistSetColumnConstraintUnlockNullWritesTxDone([[maybe_unused]] NIceDb::TNiceDb& db, [[maybe_unused]] const TSetColumnConstraintOperationInfo& operationInfo) {
    // todo
}

void TSchemeShard::PersistSetColumnConstraintUnlockTxId([[maybe_unused]] NIceDb::TNiceDb& db, [[maybe_unused]] const TSetColumnConstraintOperationInfo& operationInfo) {
    // todo
}

void TSchemeShard::PersistSetColumnConstraintUnlockTxStatus([[maybe_unused]] NIceDb::TNiceDb& db, [[maybe_unused]] const TSetColumnConstraintOperationInfo& operationInfo) {
    // todo
}

void TSchemeShard::PersistSetColumnConstraintUnlockTxDone([[maybe_unused]] NIceDb::TNiceDb& db, [[maybe_unused]] const TSetColumnConstraintOperationInfo& operationInfo) {
    // todo
}

void TSchemeShard::Handle(TEvSetColumnConstraint::TEvCreateRequest::TPtr& ev, const TActorContext& ctx) {
    Execute(CreateTxCreateSetColumnConstraint(ev), ctx);
}

} // NSchemeShard
} // NKikimr
