#pragma once

#include <ydb/services/metadata/abstract/kqp_common.h>
#include <ydb/services/metadata/manager/abstract.h>
#include <yql/essentials/sql/v1/node.h>

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

    // Properties which crated during query translation
    using TSqlSettings = NSQLTranslationV1::TStreamingQuerySettings;

    struct TProperties {
        static inline constexpr char Run[] = "run";
        static inline constexpr char ResourcePool[] = "resource_pool";
        static inline constexpr char Force[] = "force";
    };

    static NMetadata::IClassBehaviour::TPtr GetBehaviour();

    static TString GetTypeId();
};

}  // namespace NKikimr::NKqp
