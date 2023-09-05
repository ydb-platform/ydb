#pragma once

namespace NYql::NConnector {
    // List of external datasources supported by Connector.
    // Serialized values from this enum are used within
    // `CREATE EXTERNAL DATA SOURCE` syntax.
    enum class EExternalDataSource {
        ClickHouse,
        PostgreSQL
    };
}
