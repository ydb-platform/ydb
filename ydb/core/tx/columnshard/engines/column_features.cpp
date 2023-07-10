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

std::shared_ptr<NKikimr::NOlap::TColumnLoader> TColumnFeatures::GetLoader(const TIndexInfo& info) const {
    if (!LoaderCache) {
        NArrow::NTransformation::ITransformer::TPtr transformer = GetLoadTransformer();
        auto schema = info.GetColumnSchema(ColumnId);
        if (!transformer) {
            LoaderCache = std::make_shared<TColumnLoader>(transformer,
                std::make_shared<NArrow::NSerialization::TBatchPayloadDeserializer>(schema),
                schema, ColumnId);
        } else {
            LoaderCache = std::make_shared<TColumnLoader>(transformer,
                std::make_shared<NArrow::NSerialization::TFullDataDeserializer>(),
                schema, ColumnId);
        }
    }
    return LoaderCache;
}

std::optional<NKikimr::NOlap::TColumnFeatures> TColumnFeatures::BuildFromProto(const NKikimrSchemeOp::TOlapColumnDescription& columnInfo, const ui32 columnId) {
    TColumnFeatures result(columnId);
    if (columnInfo.HasCompression()) {
        auto settings = NArrow::TCompression::BuildFromProto(columnInfo.GetCompression());
        Y_VERIFY(settings.IsSuccess());
        result.Compression = *settings;
    }
    if (columnInfo.HasDictionaryEncoding()) {
        auto settings = NArrow::NDictionary::TEncodingSettings::BuildFromProto(columnInfo.GetDictionaryEncoding());
        Y_VERIFY(settings.IsSuccess());
        result.DictionaryEncoding = *settings;
    }
    return result;
}

std::unique_ptr<arrow::util::Codec> TColumnFeatures::GetCompressionCodec() const {
    if (Compression) {
        return Compression->BuildArrowCodec();
    } else {
        return nullptr;
    }
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
