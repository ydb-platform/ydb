#include "partition_split_merge_transitions_repository.h"
#include "schema.h"

namespace NKikimr::NReplication::NController {

using Schema = TControllerSchema;

bool TPartitionSplitMergeTransitionsRepository::Load(NIceDb::TNiceDb& db, TPartitionSplitMergeTransitions& transitions) {
    auto rowset = db.Table<Schema::CompletedSplitMergeTransitions>().Select();
    if (!rowset.IsReady()) {
        return false;
    }
    while (!rowset.EndOfSet()) {
        transitions.CompleteTransition(TWorkerId(
            rowset.GetValue<Schema::CompletedSplitMergeTransitions::ReplicationId>(),
            rowset.GetValue<Schema::CompletedSplitMergeTransitions::TargetId>(),
            rowset.GetValue<Schema::CompletedSplitMergeTransitions::WorkerId>()));
        if (!rowset.Next()) {
            return false;
        }
    }
    return true;
}

void TPartitionSplitMergeTransitionsRepository::SaveCompleted(NIceDb::TNiceDb& db, const TWorkerId& parent) {
    db.Table<Schema::CompletedSplitMergeTransitions>().Key(parent.ReplicationId(), parent.TargetId(), parent.WorkerId()).Update();
}

void TPartitionSplitMergeTransitionsRepository::DeleteCompleted(NIceDb::TNiceDb& db, const TVector<TWorkerId>& parents) {
    for (const auto& parent : parents) {
        db.Table<Schema::CompletedSplitMergeTransitions>().Key(parent.ReplicationId(), parent.TargetId(), parent.WorkerId()).Delete();
    }
}

}
