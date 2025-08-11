#include <util/generic/string.h>

namespace NKafka::NKafkaTransactionSql {

    TString SELECT_FOR_VALIDATION_WITHOUT_CONSUMERS = R"sql(
        --!syntax_v1
        DECLARE $Database AS Utf8;
        DECLARE $TransactionalId AS Utf8;

        SELECT * FROM `<producer_state_table_name>`
        WHERE database = $Database 
        AND transactional_id = $TransactionalId;
    )sql";

    TString SELECT_FOR_VALIDATION_WITH_CONSUMERS = R"sql(
        --!syntax_v1
        DECLARE $Database AS Utf8;
        DECLARE $TransactionalId AS Utf8;
        DECLARE $ConsumerGroups AS List<Utf8>;

        SELECT * FROM `<producer_state_table_name>`
        WHERE database = $Database 
        AND transactional_id = $TransactionalId;

        SELECT consumer_group, MAX(generation) AS generation FROM `<consumer_state_table_name>`
        VIEW PRIMARY KEY
        WHERE database = $Database
        AND consumer_group IN COMPACT $ConsumerGroups
        GROUP BY consumer_group;
    )sql";

} // namespace NKafka::NKafkaTransactionSql
