#pragma once

#include <ydb/services/metadata/abstract/kqp_common.h>
#include <ydb/services/metadata/manager/abstract.h>

namespace NKikimr::NKqp {

class TStreamingQueryConfig {
public:
    using TStatus = NMetadata::NModifications::IOperationsManager::TYqlConclusionStatus;
    using TAsyncStatus = NThreading::TFuture<TStatus>;

    struct TColumns {
        static inline constexpr char DatabaseId[] = "database_id";
        static inline constexpr char QueryPath[] = "query_path";
        static inline constexpr char State[] = "state";
    };

    struct TProperties {
        static inline constexpr char QueryText[] = "query_text";
        static inline constexpr char Run[] = "run";
        static inline constexpr char ResourcePool[] = "resource_pool";
        static inline constexpr char Force[] = "force";

        // Internal properties
        static inline constexpr char LastOperationCase[] = "__last_operation_case";
    };

    static NMetadata::IClassBehaviour::TPtr GetBehaviour();

    static TString GetTypeId();
};

}  // namespace NKikimr::NKqp
