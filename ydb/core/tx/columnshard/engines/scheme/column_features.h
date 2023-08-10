#pragma once
#include <ydb/core/formats/arrow/compression/object.h>
#include <ydb/core/formats/arrow/dictionary/object.h>
#include <ydb/core/formats/arrow/serializer/abstract.h>
#include <ydb/core/formats/arrow/transformer/abstract.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_base.h>

namespace NKikimr::NOlap {

class TSaverContext {
private:
    TString TierName;
    std::optional<NArrow::TCompression> ExternalCompression;
public:
    const std::optional<NArrow::TCompression>& GetExternalCompression() const {
        return ExternalCompression;
    }
    TSaverContext& SetExternalCompression(const std::optional<NArrow::TCompression>& value) {
        ExternalCompression = value;
        return *this;
    }
    const TString& GetTierName() const {
        return TierName;
    }
    TSaverContext& SetTierName(const TString& value) {
        TierName = value;
        return *this;
    }
};

class TColumnSaver {
private:
    NArrow::NTransformation::ITransformer::TPtr Transformer;
    NArrow::NSerialization::ISerializer::TPtr Serializer;
public:
    TColumnSaver() = default;
    TColumnSaver(NArrow::NTransformation::ITransformer::TPtr transformer, NArrow::NSerialization::ISerializer::TPtr serializer)
        : Transformer(transformer)
        , Serializer(serializer) {
        Y_VERIFY(Serializer);
    }

    TString Apply(std::shared_ptr<arrow::Array> data, std::shared_ptr<arrow::Field> field) const {
        auto schema = std::make_shared<arrow::Schema>(arrow::FieldVector{field});
        auto batch = arrow::RecordBatch::Make(schema, data->length(), {data});
        return Apply(batch);
    }

    TString Apply(const std::shared_ptr<arrow::RecordBatch>& data) const {
        Y_VERIFY(Serializer);
        if (Transformer) {
            return Serializer->Serialize(Transformer->Transform(data));
        } else {
            return Serializer->Serialize(data);
        }
    }
};

class TColumnLoader {
private:
    NArrow::NTransformation::ITransformer::TPtr Transformer;
    NArrow::NSerialization::IDeserializer::TPtr Deserializer;
    std::shared_ptr<arrow::Schema> ExpectedSchema;
    const ui32 ColumnId;
public:
    TString DebugString() const;

    TColumnLoader(NArrow::NTransformation::ITransformer::TPtr transformer, NArrow::NSerialization::IDeserializer::TPtr deserializer,
        const std::shared_ptr<arrow::Schema>& expectedSchema, const ui32 columnId)
        : Transformer(transformer)
        , Deserializer(deserializer)
        , ExpectedSchema(expectedSchema)
        , ColumnId(columnId)
    {
        Y_VERIFY(ExpectedSchema);
        Y_VERIFY(Deserializer);
    }

    ui32 GetColumnId() const {
        return ColumnId;
    }

    std::shared_ptr<arrow::Schema> GetExpectedSchema() const {
        return ExpectedSchema;
    }

    arrow::Result<std::shared_ptr<arrow::RecordBatch>> Apply(const TString& data) const {
        Y_VERIFY(Deserializer);
        arrow::Result<std::shared_ptr<arrow::RecordBatch>> columnArray = Deserializer->Deserialize(data);
        if (!columnArray.ok()) {
            return columnArray;
        }
        if (Transformer) {
            return Transformer->Transform(*columnArray);
        } else {
            return columnArray;
        }
    }
};

struct TIndexInfo;

class TColumnFeatures {
private:
    const ui32 ColumnId;
    std::optional<NArrow::TCompression> Compression;
    std::optional<NArrow::NDictionary::TEncodingSettings> DictionaryEncoding;
    mutable std::shared_ptr<TColumnLoader> LoaderCache;
public:
    TColumnFeatures(const ui32 columnId)
        : ColumnId(columnId)
    {

    }
    static std::optional<TColumnFeatures> BuildFromProto(const NKikimrSchemeOp::TOlapColumnDescription& columnInfo, const ui32 columnId);

    NArrow::NTransformation::ITransformer::TPtr GetSaveTransformer() const;
    NArrow::NTransformation::ITransformer::TPtr GetLoadTransformer() const;

    std::unique_ptr<arrow::util::Codec> GetCompressionCodec() const;

    std::shared_ptr<TColumnLoader> GetLoader(const TIndexInfo& info) const;

};

} // namespace NKikimr::NOlap
