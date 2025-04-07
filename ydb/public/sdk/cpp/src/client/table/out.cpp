#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

Y_DECLARE_OUT_SPEC(, NYdb::NTable::TCopyItem, o, x) {
    return x.Out(o);
}

Y_DECLARE_OUT_SPEC(, NYdb::NTable::TIndexDescription, o, x) {
    return x.Out(o);
}

Y_DECLARE_OUT_SPEC(, NYdb::NTable::TChangefeedDescription, o, x) {
    return x.Out(o);
}

Y_DECLARE_OUT_SPEC(, NYdb::NTable::TValueSinceUnixEpochModeSettings::EUnit, o, x) {
    return NYdb::NTable::TValueSinceUnixEpochModeSettings::Out(o, x);
}

Y_DECLARE_OUT_SPEC(, NYdb::NTable::TTxSettings, o, x) {
    return x.Out(o);
}

Y_DECLARE_OUT_SPEC(, NYdb::NTable::TCreateSessionResult, o, x) {
    return x.Out(o);
}

Y_DECLARE_OUT_SPEC(, NYdb::NTable::TDescribeTableResult, o, x) {
    return x.Out(o);
}

Y_DECLARE_OUT_SPEC(, NYdb::NTable::TVectorIndexSettings::EMetric, stream, value) {
    auto convertDistance = [&] {
        switch (value) {
        case NYdb::NTable::TVectorIndexSettings::EMetric::InnerProduct:
            return "similarity: inner_product";
        case NYdb::NTable::TVectorIndexSettings::EMetric::CosineSimilarity:
            return "similarity: cosine";
        case NYdb::NTable::TVectorIndexSettings::EMetric::CosineDistance:
            return "distance: cosine";
        case NYdb::NTable::TVectorIndexSettings::EMetric::Manhattan:
            return "distance: manhattan";
        case NYdb::NTable::TVectorIndexSettings::EMetric::Euclidean:
            return "distance: euclidean";
        case NYdb::NTable::TVectorIndexSettings::EMetric::Unspecified:
            return "metric: unspecified";
        }
    };

    stream << convertDistance();
}

Y_DECLARE_OUT_SPEC(, NYdb::NTable::TVectorIndexSettings::EVectorType, stream, value) {
    auto convertVectorType = [&] {
        switch (value) {
        case NYdb::NTable::TVectorIndexSettings::EVectorType::Float:
            return "float";
        case NYdb::NTable::TVectorIndexSettings::EVectorType::Uint8:
            return "uint8";
        case NYdb::NTable::TVectorIndexSettings::EVectorType::Int8:
            return "int8";
        case NYdb::NTable::TVectorIndexSettings::EVectorType::Bit:
            return "bit";
        case NYdb::NTable::TVectorIndexSettings::EVectorType::Unspecified:
            return "unspecified";
        }
    };

    stream << convertVectorType();
}

Y_DECLARE_OUT_SPEC(, NYdb::NTable::TVectorIndexSettings, stream, value) {
    stream << 
        "{ " << value.Metric << 
        ", vector_type: " << value.VectorType << 
        ", vector_dimension: " << value.VectorDimension  << 
        " }";
}

Y_DECLARE_OUT_SPEC(, NYdb::NTable::TKMeansTreeSettings, stream, value) {
    stream << 
        "{ settings: " << value.Settings << 
        ", clusters: " << value.Clusters << 
        ", levels: " << value.Levels << 
        " }";
}
