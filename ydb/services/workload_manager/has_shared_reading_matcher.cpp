#include "has_shared_reading_matcher.h"

#include <ydb/library/yql/providers/pq/common/pq_shared_reading.h>
#include <ydb/library/yql/providers/pq/common/yql_names.h>


namespace NKikimr::NWorkloadManager {

bool UsesSharedReading(const NKqpProto::TKqpPhyQuery& phyQuery) {
    for (const auto& tx : phyQuery.GetTransactions()) {
        for (const auto& stage : tx.GetStages()) {
            for (const auto& source : stage.GetSources()) {
                if (!source.HasExternalSource()) {
                    continue;
                }
                const auto& externalSource = source.GetExternalSource();
                if (externalSource.GetType() != NYql::PqSource) {
                    continue;
                }
                if (NYql::HasSharedReading(externalSource.GetSettings())) {
                    return true;
                }
            }
        }
    }
    return false;
}

}  // namespace NKikimr::NWorkloadManager
