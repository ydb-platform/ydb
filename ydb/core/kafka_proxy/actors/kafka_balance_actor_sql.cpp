#include "kafka_balancer_actor.h"

namespace NKafka {

const TString INSERT_NEW_GROUP = R"sql(
    --!syntax_v1
    DECLARE $ConsumerGroup AS Utf8;
    DECLARE $Generation AS Uint64;
    DECLARE $State AS Uint64;
    DECLARE $Database AS Utf8;
    DECLARE $Master AS Utf8;
    DECLARE $LastHeartbeat AS Datetime;

    INSERT INTO `%s`
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
        $LastHeartbeat,
        $Master
    );
)sql";

const TString UPDATE_GROUP = R"sql(
    --!syntax_v1
    DECLARE $ConsumerGroup AS Utf8;
    DECLARE $State AS Uint64;
    DECLARE $Generation AS Uint64;
    DECLARE $Database AS Utf8;
    DECLARE $Master AS Utf8;
    DECLARE $LastHeartbeat AS Datetime;

    UPDATE `%s`
    SET
        state = $State,
        generation = $Generation,
        last_heartbeat_time = $LastHeartbeat,
        master = $Master
    WHERE consumer_group = $ConsumerGroup
      AND database = $Database;
)sql";

const TString UPDATE_GROUP_STATE_AND_PROTOCOL = R"sql(
    --!syntax_v1
    DECLARE $ConsumerGroup AS Utf8;
    DECLARE $State AS Uint64;
    DECLARE $Database AS Utf8;
    DECLARE $Protocol AS Utf8;
    DECLARE $LastHeartbeat AS Datetime;

    UPDATE `%s`
    SET
        state = $State,
        last_heartbeat_time = $LastHeartbeat,
        protocol = $Protocol
    WHERE consumer_group = $ConsumerGroup
    AND database = $Database;
)sql";

const TString INSERT_MEMBER = R"sql(
    --!syntax_v1
    DECLARE $ConsumerGroup       AS Utf8;
    DECLARE $Generation          AS Uint64;
    DECLARE $MemberId            AS Utf8;
    DECLARE $WorkerStateProto    AS String;
    DECLARE $Database            AS Utf8;
    DECLARE $LastHeartbeat AS Datetime;

    INSERT INTO `%s`
    (
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
        $LastHeartbeat,
        $WorkerStateProto,
        $Database
    );
)sql";

const TString UPSERT_ASSIGNMENTS_AND_SET_WORKING_STATE = R"sql(
    --!syntax_v1
    DECLARE $Assignments AS List<Struct<MemberId: Utf8, Assignment: Bytes>>;
    DECLARE $ConsumerGroup AS Utf8;
    DECLARE $Database AS Utf8;
    DECLARE $Generation AS Uint64;
    DECLARE $State AS Uint64;
    DECLARE $LastHeartbeat AS Datetime;

    UPSERT INTO `%s`
    SELECT
        item.MemberId AS member_id,
        item.Assignment AS assignment,
        $ConsumerGroup AS consumer_group,
        $Database AS database,
        $Generation AS generation
    FROM AS_TABLE($Assignments) AS item;

    UPDATE `%s`
    SET
        state = $State,
        last_heartbeat_time = $LastHeartbeat
    WHERE consumer_group = $ConsumerGroup
      AND database = $Database;
)sql";

const TString UPDATE_GROUPS_AND_SELECT_WORKER_STATES = R"sql(
    --!syntax_v1
    DECLARE $ConsumerGroup AS Utf8;
    DECLARE $State AS Uint64;
    DECLARE $Generation AS Uint64;
    DECLARE $Database AS Utf8;
    DECLARE $LastHeartbeat AS Datetime;

    UPDATE `%s`
    SET
        state = $State,
        last_heartbeat_time = $LastHeartbeat
    WHERE consumer_group = $ConsumerGroup
      AND database = $Database;

    SELECT worker_state_proto, member_id
    FROM `%s`
    WHERE consumer_group = $ConsumerGroup
      AND generation = $Generation
      AND database = $Database;
)sql";

const TString CHECK_GROUP_STATE = R"sql(
    --!syntax_v1
    DECLARE $ConsumerGroup AS Utf8;
    DECLARE $Database AS Utf8;

    SELECT state, generation, master, last_heartbeat_time, consumer_group, database
    FROM `%s`
    WHERE consumer_group = $ConsumerGroup
    AND database = $Database;
)sql";

const TString FETCH_ASSIGNMENTS = R"sql(
    --!syntax_v1
    DECLARE $ConsumerGroup AS Utf8;
    DECLARE $Generation AS Uint64;
    DECLARE $MemberId AS Utf8;
    DECLARE $Database AS Utf8;

    SELECT assignment
    FROM `%s`
    WHERE consumer_group = $ConsumerGroup
      AND generation = $Generation
      AND member_id = $MemberId
      AND database = $Database;
)sql";

const TString CHECK_DEAD_MEMBERS = R"sql(
    --!syntax_v1
    DECLARE $ConsumerGroup AS Utf8;
    DECLARE $Generation AS Uint64;
    DECLARE $Database AS Utf8;
    DECLARE $Deadline AS Datetime;

    SELECT COUNT(1) as cnt
    FROM `%s`
    WHERE consumer_group = $ConsumerGroup
      AND generation = $Generation
      AND database = $Database
      AND last_heartbeat_time < $Deadline;
)sql";

const TString UPDATE_TTLS = R"sql(
    --!syntax_v1
    DECLARE $ConsumerGroup AS Utf8;
    DECLARE $Generation AS Uint64;
    DECLARE $MemberId AS Utf8;
    DECLARE $Database AS Utf8;
    DECLARE $LastHeartbeat AS Datetime;
    DECLARE $UpdateGroupHeartbeat AS Bool;

    UPDATE `%s`
    SET last_heartbeat_time = $LastHeartbeat
    WHERE consumer_group = $ConsumerGroup
        AND database = $Database
        AND $UpdateGroupHeartbeat = True;

    UPDATE `%s`
    SET last_heartbeat_time = $LastHeartbeat
    WHERE consumer_group = $ConsumerGroup
      AND generation = $Generation
      AND member_id = $MemberId
      AND database = $Database;
)sql";


const TString UPDATE_TTL_LEAVE_GROUP = R"sql(
    --!syntax_v1
    DECLARE $ConsumerGroup AS Utf8;
    DECLARE $MemberId AS Utf8;
    DECLARE $Database AS Utf8;
    DECLARE $LastHeartbeat AS Datetime;

    UPDATE `%s`
    SET last_heartbeat_time = $LastHeartbeat
    WHERE consumer_group = $ConsumerGroup
    AND member_id = $MemberId
    AND database = $Database;
)sql";

} // namespace NKafka

// savnik check max members count
