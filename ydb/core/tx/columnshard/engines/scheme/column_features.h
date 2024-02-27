#pragma once
#include <ydb/core/formats/arrow/dictionary/object.h>
#include <ydb/core/formats/arrow/serializer/abstract.h>
#include <ydb/core/formats/arrow/transformer/abstract.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/storage.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/storages_manager.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_base.h>
#include <ydb/core/formats/arrow/common/validation.h>

namespace NKikimr::NOlap {

class TSaverContext {
private:
    TString TierName;
    std::optional<NArrow::NSerialization::TSerializerContainer> ExternalSerializer;
    YDB_READONLY_DEF(std::shared_ptr<IBlobsStorageOperator>, StorageOperator);
    YDB_READONLY_DEF(std::shared_ptr<IStoragesManager>, StoragesManager);
public:
    TSaverContext(const std::shared_ptr<IStoragesManager>& storagesManager)
        : StoragesManager(storagesManager) {

    }

    const std::optional<NArrow::NSerialization::TSerializerContainer>& GetExternalSerializer() const {
        return ExternalSerializer;
    }
    TSaverContext& SetExternalSerializer(const std::optional<NArrow::NSerialization::TSerializerContainer>& value) {
        AFL_VERIFY(!!value);
        ExternalSerializer = value;
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
    NArrow::NSerialization::TSerializerContainer Serializer;
public:
    TColumnSaver() = default;
    TColumnSaver(NArrow::NTransformation::ITransformer::TPtr transformer, const NArrow::NSerialization::TSerializerContainer serializer)
        : Transformer(transformer)
        , Serializer(serializer) {
        Y_ABORT_UNLESS(Serializer);
    }

    bool IsHardPacker() const {
        return Serializer->IsHardPacker();
    }

    TString Apply(std::shared_ptr<arrow::Array> data, std::shared_ptr<arrow::Field> field) const {
        auto schema = std::make_shared<arrow::Schema>(arrow::FieldVector{field});
        auto batch = arrow::RecordBatch::Make(schema, data->length(), {data});
        return Apply(batch);
    }

    TString Apply(const std::shared_ptr<arrow::RecordBatch>& data) const {
        Y_ABORT_UNLESS(Serializer);
        if (Transformer) {
            return Serializer->SerializeFull(Transformer->Transform(data));
        } else {
            return Serializer->SerializePayload(data);
        }
    }
};

class TColumnLoader {
private:
    NArrow::NTransformation::ITransformer::TPtr Transformer;
    NArrow::NSerialization::TSerializerContainer Serializer;
    std::shared_ptr<arrow::Schema> ExpectedSchema;
    const ui32 ColumnId;
public:
    TString DebugString() const;

    TColumnLoader(NArrow::NTransformation::ITransformer::TPtr transformer, const NArrow::NSerialization::TSerializerContainer& serializer,
        const std::shared_ptr<arrow::Schema>& expectedSchema, const ui32 columnId)
        : Transformer(transformer)
        , Serializer(serializer)
        , ExpectedSchema(expectedSchema)
        , ColumnId(columnId) {
        Y_ABORT_UNLESS(ExpectedSchema);
        auto fieldsCountStr = ::ToString(ExpectedSchema->num_fields());
        Y_ABORT_UNLESS(ExpectedSchema->num_fields() == 1, "%s", fieldsCountStr.data());
        Y_ABORT_UNLESS(Serializer);
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
        Y_ABORT_UNLESS(Serializer);
        arrow::Result<std::shared_ptr<arrow::RecordBatch>> columnArray = 
            Transformer ? Serializer->Deserialize(data) : Serializer->Deserialize(data, ExpectedSchema);
        if (!columnArray.ok()) {
            return columnArray;
        }
        if (Transformer) {
            return Transformer->Transform(*columnArray);
        } else {
            return columnArray;
        }
    }

    std::shared_ptr<arrow::RecordBatch> ApplyVerified(const TString& data) const {
        return NArrow::TStatusValidator::GetValid(Apply(data));
    }

    std::shared_ptr<arrow::Array> ApplyVerifiedColumn(const TString& data) const {
        auto rb = ApplyVerified(data);
        AFL_VERIFY(rb->num_columns() == 1)("schema", rb->schema()->ToString());
        return rb->column(0);
    }
};

struct TIndexInfo;

class TColumnFeatures {
private:
    ui32 ColumnId;
    YDB_READONLY_DEF(std::shared_ptr<IBlobsStorageOperator>, Operator);
    YDB_READONLY_DEF(NArrow::NSerialization::TSerializerContainer, Serializer);
    std::optional<NArrow::NDictionary::TEncodingSettings> DictionaryEncoding;
    std::shared_ptr<TColumnLoader> Loader;
    NArrow::NTransformation::ITransformer::TPtr GetLoadTransformer() const;

    void InitLoader(const TIndexInfo& info);
    TColumnFeatures(const ui32 columnId, const std::shared_ptr<IBlobsStorageOperator>& blobsOperator);
public:

    TString DebugString() const {
        TStringBuilder sb;
        sb << "serializer=" << (Serializer ? Serializer->DebugString() : "NO") << ";";
        sb << "encoding=" << (DictionaryEncoding ? DictionaryEncoding->DebugString() : "NO") << ";";
        sb << "loader=" << (Loader ? Loader->DebugString() : "NO") << ";";
        return sb;
    }

    NArrow::NTransformation::ITransformer::TPtr GetSaveTransformer() const;
    static std::optional<TColumnFeatures> BuildFromProto(const NKikimrSchemeOp::TOlapColumnDescription& columnInfo, const TIndexInfo& indexInfo, const std::shared_ptr<IStoragesManager>& operators);
    static TColumnFeatures BuildFromIndexInfo(const ui32 columnId, const TIndexInfo& indexInfo, const std::shared_ptr<IBlobsStorageOperator>& blobsOperator);

    const std::shared_ptr<TColumnLoader>& GetLoader() const {
        AFL_VERIFY(Loader);
        return Loader;
    }
};

} // namespace NKikimr::NOlap
