#pragma once

#include <ydb/services/metadata/abstract/kqp_common.h>
#include <ydb/services/metadata/manager/abstract.h>

namespace NKikimr::NKqp {

class TStreamingQueryConfig {
public:
    using TStatus = NMetadata::NModifications::IOperationsManager::TYqlConclusionStatus;
    using TAsyncStatus = NThreading::TFuture<TStatus>;

    static inline const TString DatabaseIdColumn = "database_id";
    static inline const TString QueryPathColumn = "query_path";
    static inline const TString StateColumn = "state";

    static NMetadata::IClassBehaviour::TPtr GetBehaviour();

    static TString GetTypeId();
};

}  // namespace NKikimr::NKqp
