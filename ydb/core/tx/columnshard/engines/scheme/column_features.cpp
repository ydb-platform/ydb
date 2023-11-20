#include "column_features.h"
#include "index_info.h"
#include <ydb/core/formats/arrow/serializer/full.h>
#include <ydb/core/formats/arrow/serializer/batch_only.h>
#include <util/string/builder.h>

namespace NKikimr::NOlap {

NArrow::NTransformation::ITransformer::TPtr TColumnFeatures::GetSaveTransformer() const {
    NArrow::NTransformation::ITransformer::TPtr transformer;
    if (DictionaryEncoding) {
        transformer = DictionaryEncoding->BuildEncoder();
    }
    return transformer;
}

NArrow::NTransformation::ITransformer::TPtr TColumnFeatures::GetLoadTransformer() const {
    NArrow::NTransformation::ITransformer::TPtr transformer;
    if (DictionaryEncoding) {
        transformer = DictionaryEncoding->BuildDecoder();
    }
    return transformer;
}

void TColumnFeatures::InitLoader(const TIndexInfo& info) {
    NArrow::NTransformation::ITransformer::TPtr transformer = GetLoadTransformer();
    auto schema = info.GetColumnSchema(ColumnId);
    if (!transformer) {
        Loader = std::make_shared<TColumnLoader>(transformer,
            std::make_shared<NArrow::NSerialization::TBatchPayloadDeserializer>(schema),
            schema, ColumnId);
    } else {
        Loader = std::make_shared<TColumnLoader>(transformer,
            std::make_shared<NArrow::NSerialization::TFullDataDeserializer>(),
            schema, ColumnId);
    }
}

std::optional<NKikimr::NOlap::TColumnFeatures> TColumnFeatures::BuildFromProto(const NKikimrSchemeOp::TOlapColumnDescription& columnInfo, const TIndexInfo& indexInfo) {
    const ui32 columnId = columnInfo.GetId();
    TColumnFeatures result(columnId);
    if (columnInfo.HasCompression()) {
        auto settings = NArrow::TCompression::BuildFromProto(columnInfo.GetCompression());
        Y_ABORT_UNLESS(settings.IsSuccess());
        result.Compression = *settings;
    }
    if (columnInfo.HasDictionaryEncoding()) {
        auto settings = NArrow::NDictionary::TEncodingSettings::BuildFromProto(columnInfo.GetDictionaryEncoding());
        Y_ABORT_UNLESS(settings.IsSuccess());
        result.DictionaryEncoding = *settings;
    }
    result.InitLoader(indexInfo);
    return result;
}

std::unique_ptr<arrow::util::Codec> TColumnFeatures::GetCompressionCodec() const {
    if (Compression) {
        return Compression->BuildArrowCodec();
    } else {
        return nullptr;
    }
}

NKikimr::NOlap::TColumnFeatures TColumnFeatures::BuildFromIndexInfo(const ui32 columnId, const TIndexInfo& indexInfo) {
    TColumnFeatures result(columnId);
    result.InitLoader(indexInfo);
    return result;
}

TString TColumnLoader::DebugString() const {
    TStringBuilder result;
    if (ExpectedSchema) {
        result << "schema:" << ExpectedSchema->ToString() << ";";
    }
    if (Transformer) {
        result << "transformer:" << Transformer->DebugString() << ";";
    }
    if (Deserializer) {
        result << "deserializer:" << Deserializer->DebugString() << ";";
    }
    return result;
}

} // namespace NKikimr::NOlap
