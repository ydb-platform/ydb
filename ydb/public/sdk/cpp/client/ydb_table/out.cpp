#include "table.h"

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

Y_DECLARE_OUT_SPEC(, NYdb::NTable::TVectorIndexSettings::EDistance, stream, value) {
    auto convertDistance = [] (auto value) -> auto {
        switch (value) {
        case NYdb::NTable::TVectorIndexSettings::EDistance::Cosine:
            return "COSINE";
        case NYdb::NTable::TVectorIndexSettings::EDistance::Manhattan:
            return "MANHATTAN";
        case NYdb::NTable::TVectorIndexSettings::EDistance::Euclidean:
            return "EUCLIDEAN";
        case NYdb::NTable::TVectorIndexSettings::EDistance::Unknown:
            return "UNKNOWN";
        }
    };

    stream << convertDistance(value);
}

Y_DECLARE_OUT_SPEC(, NYdb::NTable::TVectorIndexSettings::ESimilarity, stream, value) {
    auto convertSimilarity = [] (auto value) -> auto {
        switch (value) {
        case NYdb::NTable::TVectorIndexSettings::ESimilarity::Cosine:
            return "COSINE";
        case NYdb::NTable::TVectorIndexSettings::ESimilarity::InnerProduct:
            return "INNER_PRODUCT";
        case NYdb::NTable::TVectorIndexSettings::ESimilarity::Unknown:
            return "UNKNOWN";
        }
    };

    stream << convertSimilarity(value);
}

Y_DECLARE_OUT_SPEC(, NYdb::NTable::TVectorIndexSettings::EVectorType, stream, value) {
    auto convertVectorType = [] (auto value) -> auto {
        switch (value) {
        case NYdb::NTable::TVectorIndexSettings::EVectorType::Float:
            return "FLOAT";
        case NYdb::NTable::TVectorIndexSettings::EVectorType::Uint8:
            return "UINT8";
        case NYdb::NTable::TVectorIndexSettings::EVectorType::Int8:
            return "INT8";
        case NYdb::NTable::TVectorIndexSettings::EVectorType::Bit:
            return "BIT";
        case NYdb::NTable::TVectorIndexSettings::EVectorType::Unknown:
            return "UNKNOWN";
        }
    };

    stream << convertVectorType(value);
}

Y_DECLARE_OUT_SPEC(, NYdb::NTable::TVectorIndexSettings, stream, value) {
    stream << "{";

    if (const auto* distance = std::get_if<NYdb::NTable::TVectorIndexSettings::EDistance>(&value.Metric)) {
        stream << " distance: " << *distance << "";
    } else if (const auto* similarity = std::get_if<NYdb::NTable::TVectorIndexSettings::ESimilarity>(&value.Metric)) {
        stream << " similarity: " << *similarity << "";
    }

    stream << ", vector_type: " << value.VectorType << "";
    stream << ", vector_dimension: " << value.VectorDimension << "";

    stream << " }";
}