#pragma once
#include <ydb/core/formats/arrow/compression/object.h>
#include <ydb/core/formats/arrow/dictionary/object.h>
#include <ydb/core/formats/arrow/serializer/abstract.h>
#include <ydb/core/formats/arrow/transformer/abstract.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/storage.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/storages_manager.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_base.h>

namespace NKikimr::NOlap {

class TSaverContext {
private:
    TString TierName;
    std::optional<NArrow::TCompression> ExternalCompression;
    YDB_READONLY_DEF(std::shared_ptr<IBlobsStorageOperator>, StorageOperator);
    YDB_READONLY_DEF(std::shared_ptr<IStoragesManager>, StoragesManager);
public:
    TSaverContext(const std::shared_ptr<IBlobsStorageOperator>& storageOperator, const std::shared_ptr<IStoragesManager>& storagesManager)
        : StorageOperator(storageOperator)
        , StoragesManager(storagesManager)
    {

    }

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
        Y_ABORT_UNLESS(Serializer);
    }

    bool IsHardPacker() const {
        return Serializer && Serializer->IsHardPacker();
    }

    TString Apply(std::shared_ptr<arrow::Array> data, std::shared_ptr<arrow::Field> field) const {
        auto schema = std::make_shared<arrow::Schema>(arrow::FieldVector{field});
        auto batch = arrow::RecordBatch::Make(schema, data->length(), {data});
        return Apply(batch);
    }

    TString Apply(const std::shared_ptr<arrow::RecordBatch>& data) const {
        Y_ABORT_UNLESS(Serializer);
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
        Y_ABORT_UNLESS(ExpectedSchema);
        auto fieldsCountStr = ::ToString(ExpectedSchema->num_fields());
        Y_ABORT_UNLESS(ExpectedSchema->num_fields() == 1, "%s", fieldsCountStr.data());
        Y_ABORT_UNLESS(Deserializer);
    }

    ui32 GetColumnId() const {
        return ColumnId;
    }

    const std::shared_ptr<arrow::Field>& GetField() const {
        return ExpectedSchema->field(0);
    }

    const std::shared_ptr<arrow::Schema>& GetExpectedSchema() const {
        return ExpectedSchema;
    }

    arrow::Result<std::shared_ptr<arrow::RecordBatch>> Apply(const TString& data) const {
        Y_ABORT_UNLESS(Deserializer);
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
    ui32 ColumnId;
    std::optional<NArrow::TCompression> Compression;
    std::optional<NArrow::NDictionary::TEncodingSettings> DictionaryEncoding;
    std::shared_ptr<TColumnLoader> Loader;

    NArrow::NTransformation::ITransformer::TPtr GetLoadTransformer() const;

    void InitLoader(const TIndexInfo& info);
    TColumnFeatures(const ui32 columnId)
        : ColumnId(columnId) {
    }
public:

    TString DebugString() const {
        TStringBuilder sb;
        sb << "compression=" << (Compression ? Compression->DebugString() : "NO") << ";";
        sb << "encoding=" << (DictionaryEncoding ? DictionaryEncoding->DebugString() : "NO") << ";";
        sb << "loader=" << (Loader ? Loader->DebugString() : "NO") << ";";
        return sb;
    }

    NArrow::NTransformation::ITransformer::TPtr GetSaveTransformer() const;
    static std::optional<TColumnFeatures> BuildFromProto(const NKikimrSchemeOp::TOlapColumnDescription& columnInfo, const TIndexInfo& indexInfo);
    static TColumnFeatures BuildFromIndexInfo(const ui32 columnId, const TIndexInfo& indexInfo);

    std::unique_ptr<arrow::util::Codec> GetCompressionCodec() const;

    const std::shared_ptr<TColumnLoader>& GetLoader() const {
        AFL_VERIFY(Loader);
        return Loader;
    }
};

} // namespace NKikimr::NOlap
