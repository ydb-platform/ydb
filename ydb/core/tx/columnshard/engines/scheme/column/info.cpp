#include "info.h"
#include <ydb/core/tx/columnshard/splitter/abstract/chunks.h>

namespace NKikimr::NOlap {

NArrow::NTransformation::ITransformer::TPtr TSimpleColumnInfo::GetSaveTransformer() const {
    NArrow::NTransformation::ITransformer::TPtr transformer;
    if (DictionaryEncoding) {
        transformer = DictionaryEncoding->BuildEncoder();
    }
    return transformer;
}

NArrow::NTransformation::ITransformer::TPtr TSimpleColumnInfo::GetLoadTransformer() const {
    NArrow::NTransformation::ITransformer::TPtr transformer;
    if (DictionaryEncoding) {
        transformer = DictionaryEncoding->BuildDecoder();
    }
    return transformer;
}

TConclusionStatus TSimpleColumnInfo::DeserializeFromProto(const NKikimrSchemeOp::TOlapColumnDescription& columnInfo)
{
    AFL_VERIFY(columnInfo.GetId() == ColumnId);
    if (columnInfo.HasSerializer()) {
        AFL_VERIFY(Serializer.DeserializeFromProto(columnInfo.GetSerializer()));
    } else if (columnInfo.HasCompression()) {
        AFL_VERIFY(Serializer.DeserializeFromProto(columnInfo.GetCompression()));
    }
    AFL_VERIFY(Serializer);
    if (columnInfo.HasDictionaryEncoding()) {
        auto settings = NArrow::NDictionary::TEncodingSettings::BuildFromProto(columnInfo.GetDictionaryEncoding());
        Y_ABORT_UNLESS(settings.IsSuccess());
        DictionaryEncoding = *settings;
    }
    Loader = std::make_shared<TColumnLoader>(GetLoadTransformer(), Serializer, ArrowSchema, ColumnId);
    return TConclusionStatus::Success();
}

TSimpleColumnInfo::TSimpleColumnInfo(const ui32 columnId, const std::shared_ptr<arrow::Field>& arrowField, const NArrow::NSerialization::TSerializerContainer& serializer,
    const bool needMinMax, const bool isSorted)
    : ColumnId(columnId)
    , ArrowField(arrowField)
    , ArrowSchema(std::make_shared<arrow::Schema>(arrow::FieldVector({arrowField})))
    , Serializer(serializer)
    , NeedMinMax(needMinMax)
    , IsSorted(isSorted)
{
    ColumnName = ArrowField->name();
    Loader = std::make_shared<TColumnLoader>(GetLoadTransformer(), Serializer, ArrowSchema, ColumnId);
}

// std::vector<std::shared_ptr<NKikimr::NOlap::IPortionDataChunk>> TSimpleColumnInfo::ActualizeColumnData(const std::vector<std::shared_ptr<IPortionDataChunk>>& source, const TSimpleColumnInfo& sourceColumnFeatures) const {
//     AFL_VERIFY(Loader);
//     const auto checkNeedActualize = [&]() {
//         if (!Serializer.IsEqualTo(sourceColumnFeatures.Serializer)) {
//             return true;
//         }
//         if (!Loader->IsEqualTo(*sourceColumnFeatures.Loader)) {
//             return true;
//         }
//         if (!!DictionaryEncoding != !!sourceColumnFeatures.DictionaryEncoding) {
//             return true;
//         }
//         if (!!DictionaryEncoding && DictionaryEncoding->IsEqualTo(*sourceColumnFeatures.DictionaryEncoding)) {
//             return true;
//         }
//         return false;
//     };
//     if (!checkNeedActualize()) {
//         return source;
//     }
//     std::vector<std::shared_ptr<IPortionDataChunk>> result;
//     for (auto&& s : source) {
//         auto data = NArrow::TStatusValidator::GetValid(sourceColumnFeatures.Loader->Apply(s->GetData()));
//         result.emplace_back(s->CopyWithAnotherBlob(GetColumnSaver().Apply(data), *this));
//     }
//     return result;
// }

} // namespace NKikimr::NOlap
