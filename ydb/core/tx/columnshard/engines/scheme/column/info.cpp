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

TConclusionStatus TSimpleColumnInfo::DeserializeFromProto(const NKikimrSchemeOp::TOlapColumnDescription& columnInfo) {
    AFL_VERIFY(columnInfo.GetId() == ColumnId);
    if (columnInfo.HasSerializer()) {
        AFL_VERIFY(Serializer.DeserializeFromProto(columnInfo.GetSerializer()));
    } else if (columnInfo.HasCompression()) {
        Serializer.DeserializeFromProto(columnInfo.GetCompression()).Validate();
    }
    if (columnInfo.HasDefaultValue()) {
        DefaultValue.DeserializeFromProto(columnInfo.GetDefaultValue()).Validate();
    }
    if (columnInfo.HasDataAccessorConstructor()) {
        AFL_VERIFY(DataAccessorConstructor.DeserializeFromProto(columnInfo.GetDataAccessorConstructor()));
    }
    AFL_VERIFY(Serializer);
    if (columnInfo.HasDictionaryEncoding()) {
        auto settings = NArrow::NDictionary::TEncodingSettings::BuildFromProto(columnInfo.GetDictionaryEncoding());
        Y_ABORT_UNLESS(settings.IsSuccess());
        DictionaryEncoding = *settings;
    }
    Loader = std::make_shared<TColumnLoader>(GetLoadTransformer(), Serializer, DataAccessorConstructor, ArrowField, DefaultValue.GetValue(), ColumnId);
    return TConclusionStatus::Success();
}

TSimpleColumnInfo::TSimpleColumnInfo(const ui32 columnId, const std::shared_ptr<arrow::Field>& arrowField, const NArrow::NSerialization::TSerializerContainer& serializer,
    const bool needMinMax, const bool isSorted,
    const std::shared_ptr<arrow::Scalar>& defaultValue)
    : ColumnId(columnId)
    , ArrowField(arrowField)
    , Serializer(serializer)
    , NeedMinMax(needMinMax)
    , IsSorted(isSorted)
    , DefaultValue(defaultValue)
{
    ColumnName = ArrowField->name();
    Loader = std::make_shared<TColumnLoader>(
        GetLoadTransformer(), Serializer, DataAccessorConstructor, ArrowField, DefaultValue.GetValue(), ColumnId);
}

std::vector<std::shared_ptr<NKikimr::NOlap::IPortionDataChunk>> TSimpleColumnInfo::ActualizeColumnData(const std::vector<std::shared_ptr<IPortionDataChunk>>& source, const TSimpleColumnInfo& sourceColumnFeatures) const {
    AFL_VERIFY(Loader);
    const auto checkNeedActualize = [&]() {
        if (!Serializer.IsEqualTo(sourceColumnFeatures.Serializer)) {
            AFL_NOTICE(NKikimrServices::TX_COLUMNSHARD)("event", "actualization")("reason", "serializer")
                ("from", sourceColumnFeatures.Serializer.SerializeToProto().DebugString())
                ("to", Serializer.SerializeToProto().DebugString());
            return true;
        }
        if (!Loader->IsEqualTo(*sourceColumnFeatures.Loader)) {
            AFL_NOTICE(NKikimrServices::TX_COLUMNSHARD)("event", "actualization")("reason", "loader");
            return true;
        }
        if (!!DictionaryEncoding != !!sourceColumnFeatures.DictionaryEncoding) {
            AFL_NOTICE(NKikimrServices::TX_COLUMNSHARD)("event", "actualization")("reason", "dictionary")("from", !!sourceColumnFeatures.DictionaryEncoding)("to", !!DictionaryEncoding);
            return true;
        }
        if (!!DictionaryEncoding && !DictionaryEncoding->IsEqualTo(*sourceColumnFeatures.DictionaryEncoding)) {
            AFL_NOTICE(NKikimrServices::TX_COLUMNSHARD)("event", "actualization")("reason", "dictionary_encoding")
                ("from", sourceColumnFeatures.DictionaryEncoding->SerializeToProto().DebugString())
                ("to", DictionaryEncoding->SerializeToProto().DebugString())
                ;
            return true;
        }
        return false;
    };
    if (!checkNeedActualize()) {
        return source;
    }
    std::vector<std::shared_ptr<IPortionDataChunk>> result;
    for (auto&& s : source) {
        auto data = sourceColumnFeatures.Loader->ApplyRawVerified(s->GetData());
        result.emplace_back(s->CopyWithAnotherBlob(GetColumnSaver().Apply(data), *this));
    }
    return result;
}

} // namespace NKikimr::NOlap
