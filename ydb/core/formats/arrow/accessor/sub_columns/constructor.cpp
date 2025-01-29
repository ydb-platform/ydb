#include "accessor.h"
#include "constructor.h"

namespace NKikimr::NArrow::NAccessor::NSubColumns {

TConclusion<std::shared_ptr<IChunkedArray>> TConstructor::DoConstructDefault(const TChunkConstructionData& externalInfo) const {
    AFL_VERIFY(externalInfo.GetDefaultValue() == nullptr);
    return std::make_shared<TSubColumnsArray>(externalInfo.GetColumnType(), externalInfo.GetRecordsCount());
}

TConclusion<std::shared_ptr<IChunkedArray>> TConstructor::DoDeserializeFromString(
    const TString& originalData, const TChunkConstructionData& externalInfo) const {
    NKikimrArrowAccessorProto::TSubColumnsAccessor proto;
    if (!proto.ParseFromArray(originalData.data(), originalData.size())) {
        return TConclusionStatus::Fail("cannot parse proto");
    }
    auto schema = NArrow::DeserializeSchema(proto.GetSchema().GetDescription());
    if (!schema) {
        return TConclusionStatus::Fail("cannot parse schema from proto");
    }
    if (schema->num_fields() != proto.GetColumns().size()) {
        return TConclusionStatus::Fail("inconsistency schema and columns for json proto");
    }
    std::vector<TString> columns;
    for (ui32 i = 0; i < (ui32)schema->num_fields(); ++i) {
        columns.emplace_back(proto.GetColumns(i).GetDescription());
    }
    return std::make_shared<TSubColumnsArray>(schema, std::move(columns), externalInfo);
}

NKikimrArrowAccessorProto::TConstructor TConstructor::DoSerializeToProto() const {
    NKikimrArrowAccessorProto::TConstructor result;
    *result.MutableSubColumns() = {};
    return result;
}

bool TConstructor::DoDeserializeFromProto(const NKikimrArrowAccessorProto::TConstructor& /*proto*/) {
    return true;
}

TConclusion<std::shared_ptr<IChunkedArray>> TConstructor::DoConstruct(
    const std::shared_ptr<IChunkedArray>& originalData, const TChunkConstructionData& /*externalInfo*/) const {
    return std::make_shared<NAccessor::TSubColumnsArray>(originalData, DataExtractor);
}

TString TConstructor::DoSerializeToString(const std::shared_ptr<IChunkedArray>& columnData, const TChunkConstructionData& externalInfo) const {
    const std::shared_ptr<TSubColumnsArray> arr = std::static_pointer_cast<TSubColumnsArray>(columnData);
    return arr->SerializeToString(externalInfo);
}

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns
