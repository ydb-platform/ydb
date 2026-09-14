#pragma once

#include <util/generic/fwd.h>

namespace NKafka {

    namespace NInitProducerIdSql {

        static const TString SELECT_BY_TRANSACTIONAL_ID = R"sql(
            --!syntax_v1
            DECLARE $Database AS Utf8;
            DECLARE $TransactionalId AS Utf8;

            SELECT * FROM `<table_name>`
            WHERE database = $Database AND transactional_id = $TransactionalId;
        )sql";

        static const TString INSERT_NEW_TRANSACTIONAL_ID = R"sql(
            --!syntax_v1
            DECLARE $Database AS Utf8;
            DECLARE $TransactionalId AS Utf8;
            DECLARE $ProducerEpoch AS Int16;
            DECLARE $UpdatedAt AS Datetime;

            INSERT INTO `<table_name>`
            (
                database,
                transactional_id, 
                producer_epoch,
                updated_at)
            VALUES ($Database, $TransactionalId, $ProducerEpoch, $UpdatedAt);

            SELECT * FROM `<table_name>`
            WHERE database = $Database AND transactional_id = $TransactionalId;
        )sql";

        static const TString UPDATE_PRODUCER_EPOCH = R"sql(
            --!syntax_v1
            DECLARE $Database AS Utf8;
            DECLARE $TransactionalId AS Utf8;
            DECLARE $ProducerEpoch AS Int16;
            DECLARE $UpdatedAt AS Datetime;

            UPDATE `<table_name>`
            SET producer_epoch = $ProducerEpoch,
                updated_at = $UpdatedAt
            WHERE database = $Database AND transactional_id = $TransactionalId;

            SELECT * FROM `<table_name>`
            WHERE database = $Database AND transactional_id = $TransactionalId;
        )sql";

        static const TString DELETE_BY_TRANSACTIONAL_ID = R"sql(
            --syntax_v1
            DECLARE $Database AS Utf8;
            DECLARE $TransactionalId AS Utf8;

            DELETE FROM `<table_name>`
            WHERE database = $Database AND transactional_id = $TransactionalId;
        )sql";

    } // namespace NInitProducerIdSql

} // namespace NKafka
