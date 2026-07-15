#include "kafka_offset_commit_actor.h"

namespace NKafka {

const TString CHECK_GROUP_GENERATION = R"sql(
    --!syntax_v1
    DECLARE $ConsumerGroup AS Utf8;
    DECLARE $Database AS Utf8;

    SELECT generation
    FROM `%s`
    VIEW PRIMARY KEY
    WHERE database = $Database
      AND consumer_group = $ConsumerGroup;
)sql";

}
