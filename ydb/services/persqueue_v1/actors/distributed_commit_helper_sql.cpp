#include "distributed_commit_helper.h"

namespace NKikimr::NGRpcProxy::V1 {
    const TString CHECK_GROUP_GENERATION_ID = R"sql(
        --!syntax_v1
        DECLARE $ConsumerGroup AS Utf8;
        DECLARE $Database AS Utf8;

        SELECT state, generation, last_success_generation
        FROM `%s`
        VIEW PRIMARY KEY
        WHERE database = $Database
        AND consumer_group = $ConsumerGroup;
    )sql";
}
