#pragma once 

#include <util/generic/string.h>
#include <util/generic/vector.h> 

// Fields that belongs to a warehouse
namespace Iceberg::Warehouse::Fields {

const TString TYPE         = "warehouse_type";
const TString S3_ENDPOINT  = "warehouse_s3_endpoint";
const TString S3_URI       = "warehouse_s3_uri";
const TString S3_REGION    = "warehouse_s3_region";
const TString TLS          = "use_tls";
const TString DB           = "database_name";

}

namespace Iceberg::Warehouse {

const TString S3 = "s3";

}

// Fields that belongs to a catalog 
namespace Iceberg::Catalog::Fields {

const TString TYPE     = "catalog_type";
const TString HIVE_URI = "catalog_hive_uri";

}

namespace Iceberg::Catalog {

const TString HIVE   = "hive";
const TString HADOOP = "hadoop";

}

namespace Iceberg {

// List of fields which is pass to connector     
const std::array<const TString, 6> FieldsToConnector = {
    Iceberg::Warehouse::Fields::TYPE,
    Iceberg::Warehouse::Fields::S3_ENDPOINT,
    Iceberg::Warehouse::Fields::S3_REGION,
    Iceberg::Warehouse::Fields::S3_URI,
    Iceberg::Catalog::Fields::TYPE,
    Iceberg::Catalog::Fields::HIVE_URI
};

}