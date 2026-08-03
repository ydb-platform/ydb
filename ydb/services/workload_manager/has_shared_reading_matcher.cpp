#include "has_shared_reading_matcher.h"

#include <ydb/library/yql/providers/pq/proto/dq_io.pb.h>

#include <google/protobuf/any.pb.h>


namespace NKikimr::NWorkloadManager {

bool UsesSharedReading(const NKqpProto::TKqpPhyQuery& phyQuery) {
    for (const auto& tx : phyQuery.GetTransactions()) {
        for (const auto& stage : tx.GetStages()) {
            for (const auto& source : stage.GetSources()) {
                if (!source.HasExternalSource()) {
                    continue;
                }
                const auto& externalSource = source.GetExternalSource();
                if (!externalSource.GetSettings().Is<NYql::NPq::NProto::TDqPqTopicSource>()) {
                    continue;
                }
                NYql::NPq::NProto::TDqPqTopicSource pqSource;
                if (!externalSource.GetSettings().UnpackTo(&pqSource)) {
                    continue;
                }
                if (pqSource.GetSharedReading()) {
                    return true;
                }
            }
        }
    }
    return false;
}

}  // namespace NKikimr::NWorkloadManager
