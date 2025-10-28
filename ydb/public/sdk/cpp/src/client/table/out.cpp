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

Y_DECLARE_OUT_SPEC(, NYdb::NTable::TFulltextIndexSettings::ELayout, stream, value) {
    stream << "{ ";
    switch (value) {
        case NYdb::NTable::TFulltextIndexSettings::ELayout::Flat:
            stream << "flat";
            break;
        case NYdb::NTable::TFulltextIndexSettings::ELayout::Unspecified:
            stream << "unspecified";
            break;
    }
    stream << " }";
}

Y_DECLARE_OUT_SPEC(, NYdb::NTable::TFulltextIndexSettings::ETokenizer, stream, value) {
    stream << "{ ";
    switch (value) {
        case NYdb::NTable::TFulltextIndexSettings::ETokenizer::Whitespace:
            stream << "whitespace";
            break;
        case NYdb::NTable::TFulltextIndexSettings::ETokenizer::Standard:
            stream << "standard";
            break;
        case NYdb::NTable::TFulltextIndexSettings::ETokenizer::Keyword:
            stream << "keyword";
            break;
        case NYdb::NTable::TFulltextIndexSettings::ETokenizer::Unspecified:
            stream << "unspecified";
            break;
    }
    stream << " }";
}

Y_DECLARE_OUT_SPEC(, NYdb::NTable::TFulltextIndexSettings::TAnalyzers, stream, value) {
    stream << "{ tokenizer: " << value.Tokenizer;
    if (value.Language.has_value()) {
        stream << ", language: " << *value.Language;
    }
    if (value.UseFilterLowercase.has_value()) {
        stream << ", use_filter_lowercase: " << (*value.UseFilterLowercase ? "true" : "false");
    }
    if (value.UseFilterStopwords.has_value()) {
        stream << ", use_filter_stopwords: " << (*value.UseFilterStopwords ? "true" : "false");
    }
    if (value.UseFilterNgram.has_value()) {
        stream << ", use_filter_ngram: " << (*value.UseFilterNgram ? "true" : "false");
    }
    if (value.UseFilterEdgeNgram.has_value()) {
        stream << ", use_filter_edge_ngram: " << (*value.UseFilterEdgeNgram ? "true" : "false");
    }
    if (value.FilterNgramMinLength.has_value()) {
        stream << ", filter_ngram_min_length: " << *value.FilterNgramMinLength;
    }
    if (value.FilterNgramMaxLength.has_value()) {
        stream << ", filter_ngram_max_length: " << *value.FilterNgramMaxLength;
    }
    if (value.UseFilterLength.has_value()) {
        stream << ", use_filter_length: " << (*value.UseFilterLength ? "true" : "false");
    }
    if (value.FilterLengthMin.has_value()) {
        stream << ", filter_length_min: " << *value.FilterLengthMin;
    }
    if (value.FilterLengthMax.has_value()) {
        stream << ", filter_length_max: " << *value.FilterLengthMax;
    }
    stream << " }";
}

Y_DECLARE_OUT_SPEC(, NYdb::NTable::TFulltextIndexSettings::TColumnAnalyzers, stream, value) {
    stream << "{ column: " << value.Column << ", ";
    stream << "analyzers: " << value.Analyzers << " }";
}

Y_DECLARE_OUT_SPEC(, NYdb::NTable::TFulltextIndexSettings, stream, value) {
    stream << "{ layout: " << value.Layout;
    if (!value.Columns.empty()) {
        stream << ", columns: [";
        for (size_t i = 0; i < value.Columns.size(); ++i) {
            if (i > 0) stream << ", ";
            stream << value.Columns[i];
        }
        stream << "]";
    }
    stream << " }";
}
