#pragma once 

#include <util/generic/string.h>
#include <util/generic/vector.h> 

namespace NKikimr::NExternalSource::NIceberg {

// Fields that belongs to a warehouse
constexpr char WAREHOUSE_TYPE[]         = "warehouse_type";
constexpr char WAREHOUSE_S3_ENDPOINT[]  = "warehouse_s3_endpoint";
constexpr char WAREHOUSE_S3_URI[]       = "warehouse_s3_uri";
constexpr char WAREHOUSE_S3_REGION[]    = "warehouse_s3_region";
constexpr char WAREHOUSE_TLS[]          = "use_tls";
constexpr char WAREHOUSE_DB[]           = "database_name";

// Fields that belongs to a catalog
constexpr char CATALOG_TYPE[]               = "catalog_type";
constexpr char CATALOG_HIVE_METASTORE_URI[] = "catalog_hive_metastore_uri";

// Some values 
constexpr char VALUE_S3[]               = "s3";
constexpr char VALUE_HIVE_METASTORE[]   = "hive_metastore";
constexpr char VALUE_HADOOP[]           = "hadoop";

// List of fields which is pass to a connector
constexpr std::array<const char*, 6> FieldsToConnector = {
    WAREHOUSE_TYPE,
    WAREHOUSE_S3_ENDPOINT,
    WAREHOUSE_S3_REGION,
    WAREHOUSE_S3_URI,
    CATALOG_TYPE,
    CATALOG_HIVE_METASTORE_URI
};

} // NKikimr::NExternalSource::NIceberg
