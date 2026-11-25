#pragma once

#include <util/system/types.h>

namespace NYql {

enum class EStatementKind: ui8 {
    First = 0,
    // SQLv1: INSERT INTO @tmp, DROP TABLE @tmp, MATERIALIZE.
    // Anonymous-table operations are not supported by the PG and Spark translators.
    InnerDML = First,
    // SQLv1: INSERT/UPSERT/REPLACE, UPDATE, DELETE, TRUNCATE on non-anonymous tables.
    // PG: INSERT, UPDATE, DELETE. Spark: INSERT INTO, INSERT OVERWRITE.
    DML = 1,
    // SQLv1: CREATE/ALTER/DROP TABLE, SHOW CREATE TABLE, CHECKSUM TABLE.
    // PG: CREATE/ALTER/DROP TABLE, CREATE VIEW. Spark does not currently support DDL.
    DDL = 2,
    // SQLv1: COMMIT, ROLLBACK. PG: BEGIN, COMMIT, ROLLBACK.
    // Spark does not currently support transaction-control statements.
    TxControl = 3,
    Last = TxControl,
};

} // namespace NYql
