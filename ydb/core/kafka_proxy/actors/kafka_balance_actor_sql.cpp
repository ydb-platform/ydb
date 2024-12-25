#include "kafka_balancer_actor.h"

namespace NKafka {

// savnik add db

const TString SELECT_STATE_AND_GENERATION = R"(
    --!syntax_v1
    DECLARE $ConsumerGroup AS Utf8;

    SELECT state, generation
    FROM kafka_connect_groups
    WHERE consumer_group = $ConsumerGroup;
)";

const TString INSERT_NEW_GROUP = R"(
    --!syntax_v1
    DECLARE $ConsumerGroup AS Utf8;
    DECLARE $Generation AS Uint64;
    DECLARE $State AS Uint64;

    INSERT INTO kafka_connect_groups
    (
        consumer_group,
        generation,
        state,
        current_generation_start_time
    )
    VALUES
    (
        $ConsumerGroup,
        $Generation,
        $State,
        CurrentUtcDateTime()
    );
)";

const TString UPDATE_GROUP = R"(
    --!syntax_v1
    DECLARE $ConsumerGroup AS Utf8;
    DECLARE $NewState AS Uint64;
    DECLARE $OldGeneration AS Uint64;

    UPDATE kafka_connect_groups
    SET
        state = $NewState,
        generation = $OldGeneration + 1
    WHERE consumer_group = $ConsumerGroup;
)";

const TString SELECT_MASTER = R"(
    --!syntax_v1
    DECLARE $ConsumerGroup AS Utf8;
    DECLARE $Generation AS Uint64;

    SELECT member_id
    FROM kafka_connect_members
    WHERE consumer_group = $ConsumerGroup
      AND generation = $Generation
    ORDER BY join_time
    LIMIT 1;
)";

const TString INSERT_MEMBER_AND_SELECT_MASTER = R"(
    --!syntax_v1
    DECLARE $ConsumerGroup  AS Utf8;
    DECLARE $Generation     AS Uint64;
    DECLARE $MemberId       AS Utf8;
    DECLARE $WorkerState    AS String;

    INSERT INTO kafka_connect_members (
        consumer_group,
        generation,
        member_id,
        join_time,
        hearbeat_deadline,
        worker_state
    )
    VALUES (
        $ConsumerGroup,
        $Generation,
        $MemberId,
        CurrentUtcDateTime(),
        CurrentUtcDateTime() + Interval("PT5S"),
        $WorkerState
    );

    SELECT member_id AS master_id
    FROM kafka_connect_members
    WHERE consumer_group = $ConsumerGroup
      AND generation = $Generation
    ORDER BY join_time
    LIMIT 1;
)";


// savnik Леша говорил про пагинацию

const TString UPSERT_ASSIGNMENTS_AND_SET_WORKING_STATE = R"(
    --!syntax_v1
    DECLARE $Assignments AS List<Struct<MemberId: Utf8, Assignment: Bytes>>;
    DECLARE $ConsumerGroup AS Utf8;

    UPSERT INTO kafka_connect_members
    SELECT
        item.MemberId AS member_id,
        item.Assignment AS assignment,
        $ConsumerGroup AS consumer_group
    FROM AS_TABLE($Assignments) AS item;

    UPDATE kafka_connect_groups
    SET state = 2
    WHERE consumer_group = $ConsumerGroup;

)";

const TString UPDATE_GROUPS_AND_SELECT_WORKER_STATES = R"(
    --!syntax_v1
    DECLARE $ConsumerGroup AS Utf8;
    DECLARE $State AS Uint64;
    DECLARE $Generation AS Uint64;

    UPDATE kafka_connect_groups
    SET state = $State
    WHERE consumer_group = $ConsumerGroup;

    SELECT worker_state
    FROM kafka_connect_members
    WHERE consumer_group = $ConsumerGroup
    AND generation = $Generation;
)";

const TString CHECK_GROUP_STATE = R"(
    --!syntax_v1
    DECLARE $ConsumerGroup AS Utf8;

    SELECT state, generation
    FROM kafka_connect_groups
    WHERE consumer_group = $ConsumerGroup;
)";

const TString FETCH_ASSIGNMENT = R"(
    --!syntax_v1
    DECLARE $ConsumerGroup AS Utf8;
    DECLARE $Generation AS Uint64;
    DECLARE $MemberId AS Utf8;

    SELECT assignment
    FROM kafka_connect_members
    WHERE consumer_group = $ConsumerGroup
      AND generation = $Generation
      AND member_id = $MemberId;
)";

} // namespace NKafka
