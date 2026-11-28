#pragma once

#include "partition_split_merge_transitions.h"

#include <ydb/core/tablet_flat/flat_cxx_database.h>

namespace NKikimr::NReplication::NController {

class TPartitionSplitMergeTransitionsRepository {
public:
    static bool Load(NIceDb::TNiceDb& db, TPartitionSplitMergeTransitions& transitions);
    static void SaveCompleted(NIceDb::TNiceDb& db, const TWorkerId& parent);
    static void DeleteCompleted(NIceDb::TNiceDb& db, const TVector<TWorkerId>& parents);
};

}
