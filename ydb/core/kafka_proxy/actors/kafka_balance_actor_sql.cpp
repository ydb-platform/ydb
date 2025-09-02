#include "kafka_balancer_actor.h"

namespace NKafka {

const TString INSERT_NEW_GROUP = R"sql(
    --!syntax_v1
    DECLARE $ConsumerGroup AS Utf8;
    DECLARE $Generation AS Uint64;
    DECLARE $State AS Uint64;
    DECLARE $Database AS Utf8;
    DECLARE $Master AS Utf8;
    DECLARE $LastMasterHeartbeat AS Datetime;
    DECLARE $ProtocolType AS Utf8;

    INSERT INTO `%s`
    (
        consumer_group,
        generation,
        state,
        database,
        last_heartbeat_time,
        master,
        protocol_type
    )
    VALUES
    (
        $ConsumerGroup,
        $Generation,
        $State,
        $Database,
        $LastMasterHeartbeat,
        $Master,
        $ProtocolType
    );
)sql";

const TString UPDATE_GROUP = R"sql(
    --!syntax_v1
    DECLARE $ConsumerGroup AS Utf8;
    DECLARE $State AS Uint64;
    DECLARE $Generation AS Uint64;
    DECLARE $Database AS Utf8;
    DECLARE $Master AS Utf8;
    DECLARE $LastMasterHeartbeat AS Datetime;

    UPDATE `%s`
    SET
        state = $State,
        generation = $Generation,
        last_heartbeat_time = $LastMasterHeartbeat,
        master = $Master
    WHERE database = $Database
      AND consumer_group = $ConsumerGroup;
)sql";

const TString UPDATE_GROUP_STATE = R"sql(
    --!syntax_v1
    DECLARE $ConsumerGroup AS Utf8;
    DECLARE $Database AS Utf8;
    DECLARE $State AS Uint64;
    DECLARE $Generation AS Uint64;

    UPDATE `%s`
    SET
        state = $State
    WHERE database = $Database
      AND consumer_group = $ConsumerGroup
      AND generation = $Generation;
)sql";

const TString UPDATE_GROUP_STATE_AND_PROTOCOL = R"sql(
    --!syntax_v1
    DECLARE $ConsumerGroup AS Utf8;
    DECLARE $State AS Uint64;
    DECLARE $Database AS Utf8;
    DECLARE $Protocol AS Utf8;
    DECLARE $LastMasterHeartbeat AS Datetime;

    UPDATE `%s`
    SET
        state = $State,
        last_heartbeat_time = $LastMasterHeartbeat,
        protocol = $Protocol
    WHERE database = $Database
    AND consumer_group = $ConsumerGroup;
)sql";

const TString INSERT_MEMBER = R"sql(
    --!syntax_v1
    DECLARE $ConsumerGroup       AS Utf8;
    DECLARE $Generation          AS Uint64;
    DECLARE $MemberId            AS Utf8;
    DECLARE $InstanceId          AS Utf8;
    DECLARE $WorkerStateProto    AS String;
    DECLARE $Database            AS Utf8;
    DECLARE $HeartbeatDeadline AS Datetime;
    DECLARE $SessionTimeoutMs AS Uint32;
    DECLARE $RebalanceTimeoutMs AS Uint32;

    INSERT INTO `%s`
    (
        consumer_group,
        generation,
        member_id,
        instance_id,
        heartbeat_deadline,
        worker_state_proto,
        database,
        session_timeout_ms,
        rebalance_timeout_ms
    )
    VALUES (
        $ConsumerGroup,
        $Generation,
        $MemberId,
        $InstanceId,
        $HeartbeatDeadline,
        $WorkerStateProto,
        $Database,
        $SessionTimeoutMs,
        $RebalanceTimeoutMs
    );
)sql";

const TString UPSERT_ASSIGNMENTS_AND_SET_WORKING_STATE = R"sql(
    --!syntax_v1
    DECLARE $Assignments AS List<Struct<MemberId: Utf8, Assignment: Bytes>>;
    DECLARE $ConsumerGroup AS Utf8;
    DECLARE $Database AS Utf8;
    DECLARE $Generation AS Uint64;
    DECLARE $State AS Uint64;
    DECLARE $LastMasterHeartbeat AS Datetime;

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
        last_heartbeat_time = $LastMasterHeartbeat,
        last_success_generation = $Generation
    WHERE database = $Database
      AND consumer_group = $ConsumerGroup;
)sql";

const TString SELECT_ALIVE_MEMBERS = R"sql(
    --!syntax_v1
    DECLARE $ConsumerGroup AS Utf8;
    DECLARE $Generation AS Uint64;
    DECLARE $Database AS Utf8;
    DECLARE $PaginationMemberId AS Utf8;
    DECLARE $Limit AS Uint64;

    SELECT member_id, instance_id, rebalance_timeout_ms, session_timeout_ms, heartbeat_deadline
    FROM `%s`
    VIEW PRIMARY KEY
    WHERE database = $Database
      AND consumer_group = $ConsumerGroup
      AND generation = $Generation
      AND member_id > $PaginationMemberId
      AND (leaved IS NULL OR leaved = False)
    ORDER BY member_id
    LIMIT $Limit;
)sql";

const TString SELECT_WORKER_STATES = R"sql(
    --!syntax_v1
    DECLARE $ConsumerGroup AS Utf8;
    DECLARE $Generation AS Uint64;
    DECLARE $Database AS Utf8;
    DECLARE $PaginationMemberId AS Utf8;
    DECLARE $Limit AS Uint64;

    SELECT worker_state_proto, member_id, instance_id
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

    SELECT state, generation, master, last_heartbeat_time, consumer_group, database, protocol, protocol_type, last_success_generation
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
    DECLARE $Now AS Datetime;

    SELECT COUNT(1) deads_cnt
    FROM `%s`
    VIEW idx_group_generation_db_hb
    WHERE database = $Database
    AND consumer_group = $ConsumerGroup
    AND generation = $Generation
    AND heartbeat_deadline < $Now;

    SELECT session_timeout_ms
    FROM `%s`
    VIEW PRIMARY KEY
    WHERE database = $Database
      AND consumer_group = $ConsumerGroup
      AND generation = $Generation
      AND member_id = $MemberId;

)sql";

const TString UPDATE_LAST_MEMBER_AND_GROUP_HEARTBEATS = R"sql(
    --!syntax_v1
    DECLARE $ConsumerGroup AS Utf8;
    DECLARE $Generation AS Uint64;
    DECLARE $MemberId AS Utf8;
    DECLARE $Database AS Utf8;
    DECLARE $LastMasterHeartbeat AS Datetime;
    DECLARE $HeartbeatDeadline AS Datetime;
    DECLARE $UpdateGroupHeartbeat AS Bool;

    UPDATE `%s`
    SET last_heartbeat_time = $LastMasterHeartbeat
    WHERE consumer_group = $ConsumerGroup
        AND database = $Database
        AND generation = $Generation
        AND $UpdateGroupHeartbeat = True;

    UPDATE `%s`
    SET heartbeat_deadline = $HeartbeatDeadline
    WHERE consumer_group = $ConsumerGroup
      AND generation = $Generation
      AND member_id = $MemberId
      AND database = $Database;
)sql";

const TString UPDATE_LAST_MEMBER_HEARTBEAT = R"sql(
    --!syntax_v1
    DECLARE $ConsumerGroup AS Utf8;
    DECLARE $Generation AS Uint64;
    DECLARE $MemberId AS Utf8;
    DECLARE $Database AS Utf8;
    DECLARE $HeartbeatDeadline AS Datetime;

    UPDATE `%s`
    SET heartbeat_deadline = $HeartbeatDeadline
    WHERE consumer_group = $ConsumerGroup
      AND generation = $Generation
      AND member_id = $MemberId
      AND database = $Database;
)sql";

const TString CHECK_MASTER_ALIVE = R"sql(
    --!syntax_v1
    DECLARE $ConsumerGroup AS Utf8;
    DECLARE $MasterId AS Utf8;
    DECLARE $Database AS Utf8;
    DECLARE $Generation AS Uint64;
    DECLARE $Now AS Datetime;

    SELECT COUNT(1) allive,
    FROM `%s`
    VIEW PRIMARY KEY
    WHERE database = $Database
     AND consumer_group = $ConsumerGroup
     AND generation = $Generation
     AND member_id = $MasterId
     AND heartbeat_deadline > $Now;
)sql";

const TString GET_GENERATION_BY_MEMBER = R"sql(
    --!syntax_v1
    DECLARE $ConsumerGroup AS Utf8;
    DECLARE $MemberId AS Utf8;
    DECLARE $Database AS Utf8;

    SELECT generation
    FROM `%s`
    VIEW PRIMARY KEY
    WHERE database = $Database
     AND consumer_group = $ConsumerGroup
     AND member_id = $MemberId
     ORDER BY generation DESC
     LIMIT 1;
)sql";

const TString UPDATE_LAST_HEARTBEAT_AND_STATE_TO_LEAVE_GROUP = R"sql(
    --!syntax_v1
    DECLARE $ConsumerGroup AS Utf8;
    DECLARE $MemberId AS Utf8;
    DECLARE $Database AS Utf8;
    DECLARE $Generation AS Uint64;
    DECLARE $LastMasterHeartbeat AS Datetime;
    DECLARE $State AS Uint64;
    DECLARE $UpdateState AS Bool;

    UPDATE `%s`
    SET heartbeat_deadline = $LastMasterHeartbeat,
        leaved = True
    WHERE database = $Database
     AND consumer_group = $ConsumerGroup
     AND generation = $Generation
     AND member_id = $MemberId;

    UPDATE `%s`
    SET
        state = $State
    WHERE database = $Database
      AND consumer_group = $ConsumerGroup
      AND $UpdateState = True;
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
