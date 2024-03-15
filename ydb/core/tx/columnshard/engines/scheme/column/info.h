#pragma once
#include <ydb/core/tx/columnshard/engines/scheme/abstract/loader.h>
#include <ydb/core/tx/columnshard/engines/scheme/abstract/saver.h>

#include <ydb/core/formats/arrow/dictionary/object.h>
#include <ydb/core/formats/arrow/serializer/abstract.h>
#include <ydb/core/formats/arrow/transformer/abstract.h>
#include <ydb/core/formats/arrow/common/validation.h>

#include <ydb/library/accessor/accessor.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_base.h>

namespace NKikimr::NOlap {

class IPortionDataChunk;

class TSimpleColumnInfo {
private:
    YDB_READONLY(ui32, ColumnId, 0);
    YDB_READONLY_DEF(TString, ColumnName);
    YDB_READONLY_DEF(std::shared_ptr<arrow::Field>, ArrowField);
    YDB_READONLY_DEF(std::shared_ptr<arrow::Schema>, ArrowSchema);
    YDB_READONLY(NArrow::NSerialization::TSerializerContainer, Serializer, NArrow::NSerialization::TSerializerContainer::GetDefaultSerializer());
    YDB_READONLY(bool, NeedMinMax, false);
    YDB_READONLY(bool, IsSorted, false);
    std::optional<NArrow::NDictionary::TEncodingSettings> DictionaryEncoding;
    std::shared_ptr<TColumnLoader> Loader;
    NArrow::NTransformation::ITransformer::TPtr GetLoadTransformer() const;

public:

    TSimpleColumnInfo(const ui32 columnId, const std::shared_ptr<arrow::Field>& arrowField, const NArrow::NSerialization::TSerializerContainer& serializer, const bool needMinMax, const bool isSorted);

    TColumnSaver GetColumnSaver() const {
        NArrow::NTransformation::ITransformer::TPtr transformer = GetSaveTransformer();
        AFL_VERIFY(Serializer);
        return TColumnSaver(transformer, Serializer);
    }

//    std::vector<std::shared_ptr<IPortionDataChunk>> ActualizeColumnData(const std::vector<std::shared_ptr<IPortionDataChunk>>& source, const TSimpleColumnInfo& sourceColumnFeatures) const;

    TString DebugString() const {
        TStringBuilder sb;
        sb << "serializer=" << (Serializer ? Serializer->DebugString() : "NO") << ";";
        sb << "encoding=" << (DictionaryEncoding ? DictionaryEncoding->DebugString() : "NO") << ";";
        sb << "loader=" << (Loader ? Loader->DebugString() : "NO") << ";";
        return sb;
    }

    NArrow::NTransformation::ITransformer::TPtr GetSaveTransformer() const;
    TConclusionStatus DeserializeFromProto(const NKikimrSchemeOp::TOlapColumnDescription& columnInfo);

    const std::shared_ptr<TColumnLoader>& GetLoader() const {
        AFL_VERIFY(Loader);
        return Loader;
    }
};

} // namespace NKikimr::NOlap
