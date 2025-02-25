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
    DECLARE $ProtocolType AS Utf8;
    DECLARE $RebalanceTimeoutMs AS Uint32;

    INSERT INTO `%s`
    (
        consumer_group,
        generation,
        state,
        database,
        last_heartbeat_time,
        master,
        protocol_type,
        rebalance_timeout_ms
    )
    VALUES
    (
        $ConsumerGroup,
        $Generation,
        $State,
        $Database,
        $LastHeartbeat,
        $Master,
        $ProtocolType,
        $RebalanceTimeoutMs
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
    DECLARE $RebalanceTimeoutMs AS Uint32;

    UPDATE `%s`
    SET
        state = $State,
        generation = $Generation,
        last_heartbeat_time = $LastHeartbeat,
        master = $Master,
        rebalance_timeout_ms = $RebalanceTimeoutMs
    WHERE database = $Database
      AND consumer_group = $ConsumerGroup;
)sql";

const TString UPDATE_GROUP_STATE = R"sql(
    --!syntax_v1
    DECLARE $ConsumerGroup AS Utf8;
    DECLARE $Database AS Utf8;
    DECLARE $State AS Uint64;

    UPDATE `%s`
    SET
        state = $State
    WHERE database = $Database
      AND consumer_group = $ConsumerGroup;
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
    WHERE database = $Database
    AND consumer_group = $ConsumerGroup;
)sql";

const TString INSERT_MEMBER = R"sql(
    --!syntax_v1
    DECLARE $ConsumerGroup       AS Utf8;
    DECLARE $Generation          AS Uint64;
    DECLARE $MemberId            AS Utf8;
    DECLARE $WorkerStateProto    AS String;
    DECLARE $Database            AS Utf8;
    DECLARE $HeartbeatDeadline AS Datetime;
    DECLARE $SessionTimeoutMs AS Uint32;

    INSERT INTO `%s`
    (
        consumer_group,
        generation,
        member_id,
        heartbeat_deadline,
        worker_state_proto,
        database,
        session_timeout_ms
    )
    VALUES (
        $ConsumerGroup,
        $Generation,
        $MemberId,
        $HeartbeatDeadline,
        $WorkerStateProto,
        $Database,
        $SessionTimeoutMs
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
    WHERE database = $Database
      AND consumer_group = $ConsumerGroup;
)sql";

const TString SELECT_WORKER_STATES = R"sql(
    --!syntax_v1
    DECLARE $ConsumerGroup AS Utf8;
    DECLARE $Generation AS Uint64;
    DECLARE $Database AS Utf8;
    DECLARE $PaginationMemberId AS Utf8;
    DECLARE $Limit AS Uint64;

    SELECT worker_state_proto, member_id
    FROM `%s`
    VIEW PRIMARY KEY
    WHERE database = $Database
      AND consumer_group = $ConsumerGroup
      AND generation = $Generation
      AND member_id > $PaginationMemberId
    ORDER BY member_id
    LIMIT $Limit;
)sql";

const TString CHECK_GROUP_STATE = R"sql(
    --!syntax_v1
    DECLARE $ConsumerGroup AS Utf8;
    DECLARE $Database AS Utf8;

    SELECT state, generation, master, last_heartbeat_time, consumer_group, database, protocol, protocol_type, rebalance_timeout_ms
    FROM `%s`
    VIEW PRIMARY KEY
    WHERE database = $Database
      AND consumer_group = $ConsumerGroup;
)sql";

const TString FETCH_ASSIGNMENTS = R"sql(
    --!syntax_v1
    DECLARE $ConsumerGroup AS Utf8;
    DECLARE $Generation AS Uint64;
    DECLARE $MemberId AS Utf8;
    DECLARE $Database AS Utf8;

    SELECT assignment
    FROM `%s`
    VIEW PRIMARY KEY
    WHERE database = $Database
      AND consumer_group = $ConsumerGroup
      AND generation = $Generation
      AND member_id = $MemberId;
)sql";

const TString CHECK_DEAD_MEMBERS = R"sql(
    --!syntax_v1
    DECLARE $ConsumerGroup AS Utf8;
    DECLARE $Generation AS Uint64;
    DECLARE $Database AS Utf8;
    DECLARE $MemberId AS Utf8;

    SELECT heartbeat_deadline
    FROM `%s`
    VIEW idx_group_generation_db_hb
    WHERE database = $Database
    AND consumer_group = $ConsumerGroup
    AND generation = $Generation
    ORDER BY heartbeat_deadline
    LIMIT 1;

    SELECT session_timeout_ms
    FROM `%s`
    VIEW PRIMARY KEY
    WHERE database = $Database
      AND consumer_group = $ConsumerGroup
      AND generation = $Generation
      AND member_id = $MemberId;

)sql";

const TString UPDATE_LASTHEARTBEATS = R"sql(
    --!syntax_v1
    DECLARE $ConsumerGroup AS Utf8;
    DECLARE $Generation AS Uint64;
    DECLARE $MemberId AS Utf8;
    DECLARE $Database AS Utf8;
    DECLARE $LastHeartbeat AS Datetime;
    DECLARE $HeartbeatDeadline AS Datetime;
    DECLARE $UpdateGroupHeartbeat AS Bool;

    UPDATE `%s`
    SET last_heartbeat_time = $LastHeartbeat
    WHERE consumer_group = $ConsumerGroup
        AND database = $Database
        AND $UpdateGroupHeartbeat = True;

    UPDATE `%s`
    SET heartbeat_deadline = $HeartbeatDeadline
    WHERE consumer_group = $ConsumerGroup
      AND generation = $Generation
      AND member_id = $MemberId
      AND database = $Database;
)sql";


const TString UPDATE_LASTHEARTBEAT_TO_LEAVE_GROUP = R"sql(
    --!syntax_v1
    DECLARE $ConsumerGroup AS Utf8;
    DECLARE $MemberId AS Utf8;
    DECLARE $Database AS Utf8;
    DECLARE $LastHeartbeat AS Datetime;

    UPDATE `%s`
    SET heartbeat_deadline = $LastHeartbeat
    WHERE consumer_group = $ConsumerGroup
    AND member_id = $MemberId
    AND database = $Database;
)sql";

const TString CHECK_GROUPS_COUNT = R"sql(
    --!syntax_v1
    DECLARE $GroupsCountCheckDeadline AS Datetime;

    SELECT COUNT(1) as groups_count
    FROM `%s`
    VIEW idx_last_hb
    WHERE last_heartbeat_time > $GroupsCountCheckDeadline;
)sql";

} // namespace NKafka
