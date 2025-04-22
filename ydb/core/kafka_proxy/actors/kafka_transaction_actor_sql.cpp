#pragma once

#include <util/generic/fwd.h>

namespace NKafka {

    namespace NKafkaTransactionSql {

        constexpr ui32 PRODUCER_STATE_REQUEST_INDEX = 0;
        constexpr ui32 CONSUMER_STATES_REQUEST_INDEX = 1;

        static const TString SELECT_FOR_VALIDATION = R"sql(
            --!syntax_v1
            DECLARE $Database AS Utf8;
            DECLARE $TransactionalId AS Utf8;
            DECLARE $ConsumerGroups AS List<Utf8>;

            SELECT * FROM `<producer_state_table_name>`
            WHERE database = $Database 
            AND transactional_id = $TransactionalId;

            SELECT consumer_group, MAX(generation) FROM `<consumer_state_table_name>`
            VIEW PRIMARY KEY
            WHERE database = $Database
            AND consumer_group IN COMPACT ($ConsumerGroups)
            GROUP BY consumer_group;
        )sql";

    } // namespace NInitProducerIdSql

} // namespace NKafka
