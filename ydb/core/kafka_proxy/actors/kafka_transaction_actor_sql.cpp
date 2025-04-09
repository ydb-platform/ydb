#pragma once

#include <util/generic/fwd.h>

namespace NKafka {

    namespace NKafkaTransactionSql {

        static const TString SELECT_FOR_VALIDATION = R"sql(
            --!syntax_v1
            DECLARE $Database AS Utf8;
            DECLARE $TransactionalId AS Utf8;
            DECLARE $ConsumerGroups AS List<Utf8>;

            SELECT * FROM `<producer_state_table_name>`
            WHERE database = $Database AND transactional_id = $TransactionalId;

            SELECT state, generation, master, last_heartbeat_time, consumer_group, database, protocol, protocol_type, last_success_generation
            FROM `<consumer_state_table_name>`
            VIEW PRIMARY KEY
            WHERE database = $Database
            AND consumer_group IN COMPACT ($ConsumerGroups);
        )sql";

    } // namespace NInitProducerIdSql

} // namespace NKafka
