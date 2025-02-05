#include "kafka_balancer_actor.h"

namespace NKafka {

const TString INSERT_NEW_GROUP = R"(
    --!syntax_v1
    DECLARE $ConsumerGroup AS Utf8;
    DECLARE $Generation AS Uint64;
    DECLARE $State AS Uint64;
    DECLARE $Database AS Utf8;
    DECLARE $Master AS Utf8;

    INSERT INTO `/Root/.metadata/kafka_consumer_groups`
    (
        consumer_group,
        generation,
        state,
        database,
        last_heartbeat_time,
        master
    )
    VALUES
    (
        $ConsumerGroup,
        $Generation,
        $State,
        $Database,
        CurrentUtcDateTime(),
        $Master
    );
)";

const TString UPDATE_GROUP = R"(
    --!syntax_v1
    DECLARE $ConsumerGroup AS Utf8;
    DECLARE $State AS Uint64;
    DECLARE $Generation AS Uint64;
    DECLARE $Database AS Utf8;
    DECLARE $Master AS Utf8;

    UPDATE `/Root/.metadata/kafka_consumer_groups`
    SET
        state = $State,
        generation = $Generation,
        last_heartbeat_time = CurrentUtcDateTime(),
        master = $Master
    WHERE consumer_group = $ConsumerGroup
      AND database = $Database;
)";

const TString UPDATE_GROUP_STATE_AND_PROTOCOL = R"(
    --!syntax_v1
    DECLARE $ConsumerGroup AS Utf8;
    DECLARE $State AS Uint64;
    DECLARE $Database AS Utf8;
    DECLARE $Protocol AS Utf8;

    UPDATE `/Root/.metadata/kafka_consumer_groups`
    SET
        state = $State,
        last_heartbeat_time = CurrentUtcDateTime(),
        protocol = $Protocol
    WHERE consumer_group = $ConsumerGroup
    AND database = $Database;
)";

const TString INSERT_MEMBER = R"(
    --!syntax_v1
    DECLARE $ConsumerGroup       AS Utf8;
    DECLARE $Generation          AS Uint64;
    DECLARE $MemberId            AS Utf8;
    DECLARE $WorkerStateProto    AS String;
    DECLARE $Database            AS Utf8;

    INSERT INTO `/Root/.metadata/kafka_consumer_members` (
        consumer_group,
        generation,
        member_id,
        last_heartbeat_time,
        worker_state_proto,
        database
    )
    VALUES (
        $ConsumerGroup,
        $Generation,
        $MemberId,
        CurrentUtcDateTime(),
        $WorkerStateProto,
        $Database
    );
)";

const TString UPSERT_ASSIGNMENTS_AND_SET_WORKING_STATE = R"(
    --!syntax_v1
    DECLARE $Assignments AS List<Struct<MemberId: Utf8, Assignment: Bytes>>;
    DECLARE $ConsumerGroup AS Utf8;
    DECLARE $Database AS Utf8;
    DECLARE $Generation AS Uint64;
    DECLARE $State AS Uint64;

    UPSERT INTO `/Root/.metadata/kafka_consumer_members`
    SELECT
        item.MemberId AS member_id,
        item.Assignment AS assignment,
        $ConsumerGroup AS consumer_group,
        $Database AS database,
        $Generation AS generation
    FROM AS_TABLE($Assignments) AS item;

    UPDATE `/Root/.metadata/kafka_consumer_groups`
    SET
        state = $State,
        last_heartbeat_time = CurrentUtcDateTime()
    WHERE consumer_group = $ConsumerGroup
      AND database = $Database;
)";

const TString UPDATE_GROUPS_AND_SELECT_WORKER_STATES = R"(
    --!syntax_v1
    DECLARE $ConsumerGroup AS Utf8;
    DECLARE $State AS Uint64;
    DECLARE $Generation AS Uint64;
    DECLARE $Database AS Utf8;

    UPDATE `/Root/.metadata/kafka_consumer_groups`
    SET
        state = $State,
        last_heartbeat_time = CurrentUtcDateTime()
    WHERE consumer_group = $ConsumerGroup
      AND database = $Database;

    SELECT worker_state_proto, member_id
    FROM `/Root/.metadata/kafka_consumer_members`
    WHERE consumer_group = $ConsumerGroup
      AND generation = $Generation
      AND database = $Database;
)";

const TString CHECK_GROUP_STATE = R"(
    --!syntax_v1
    DECLARE $ConsumerGroup AS Utf8;
    DECLARE $Database AS Utf8;

    SELECT state, generation, master, last_heartbeat_time, consumer_group, database
    FROM `/Root/.metadata/kafka_consumer_groups`
    WHERE consumer_group = $ConsumerGroup
    AND database = $Database;
)";

const TString FETCH_ASSIGNMENTS = R"(
    --!syntax_v1
    DECLARE $ConsumerGroup AS Utf8;
    DECLARE $Generation AS Uint64;
    DECLARE $MemberId AS Utf8;
    DECLARE $Database AS Utf8;

    SELECT assignment
    FROM `/Root/.metadata/kafka_consumer_members`
    WHERE consumer_group = $ConsumerGroup
      AND generation = $Generation
      AND member_id = $MemberId
      AND database = $Database;
)";

const TString CHECK_DEAD_MEMBERS = R"(
    --!syntax_v1
    DECLARE $ConsumerGroup AS Utf8;
    DECLARE $Generation AS Uint64;
    DECLARE $Database AS Utf8;
    DECLARE $Deadline AS Datetime;

    SELECT COUNT(1) as cnt
    FROM `/Root/.metadata/kafka_consumer_members`
    WHERE consumer_group = $ConsumerGroup
      AND generation = $Generation
      AND database = $Database
      AND last_heartbeat_time < $Deadline;
)";

const TString UPDATE_TTLS = R"(
    --!syntax_v1
    DECLARE $ConsumerGroup AS Utf8;
    DECLARE $Generation AS Uint64;
    DECLARE $MemberId AS Utf8;
    DECLARE $Database AS Utf8;
    DECLARE $HeartbeatDeadline AS Datetime;
    DECLARE $UpdateGroupHeartbeat AS Bool;

    UPDATE `/Root/.metadata/kafka_consumer_groups`
    SET last_heartbeat_time = CurrentUtcDateTime()
    WHERE consumer_group = $ConsumerGroup
        AND database = $Database
        AND $UpdateGroupHeartbeat = True;

    UPDATE `/Root/.metadata/kafka_consumer_members`
    SET last_heartbeat_time = $HeartbeatDeadline
    WHERE consumer_group = $ConsumerGroup
      AND generation = $Generation
      AND member_id = $MemberId
      AND database = $Database;
)";


const TString UPDATE_TTL_LEAVE_GROUP = R"(
    --!syntax_v1
    DECLARE $ConsumerGroup AS Utf8;
    DECLARE $MemberId AS Utf8;
    DECLARE $Database AS Utf8;

    UPDATE `/Root/.metadata/kafka_consumer_members`
    SET last_heartbeat_time = CurrentUtcDateTime() - Interval("PT1H")
    WHERE consumer_group = $ConsumerGroup
    AND member_id = $MemberId
    AND database = $Database;
)";


} // namespace NKafka


// savnik check max members count