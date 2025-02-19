#include "abstract.h"
#include "collection.h"

#include <util/string/join.h>

namespace NKikimr::NArrow::NSSA {

NJson::TJsonValue IResourceProcessor::DebugJson() const {
    NJson::TJsonValue result = NJson::JSON_MAP;
    if (Input.size()) {
        result.InsertValue("input", JoinSeq(",", Input));
    }
    if (Output.size()) {
        result.InsertValue("output", JoinSeq(",", Output));
    }
    result.InsertValue("type", ::ToString(ProcessorType));
    result.InsertValue("internal", DoDebugJson());
    return result;
}

TConclusionStatus IResourceProcessor::Execute(const std::shared_ptr<TAccessorsCollection>& resources) const {
    for (auto&& i : Output) {
        if (resources->HasColumn(i.GetColumnId())) {
            return TConclusionStatus::Fail("column " + ::ToString(i.GetColumnId()) + " has already");
        }
    }
    return DoExecute(resources);
}

NJson::TJsonValue TResourceProcessorStep::DebugJson() const {
    NJson::TJsonValue result = NJson::JSON_MAP;
    if (ColumnsToFetch.size()) {
        result.InsertValue("fetch", JoinSeq(",", ColumnsToFetch));
    }
    if (ColumnsToDrop.size()) {
        result.InsertValue("drop", JoinSeq(",", ColumnsToDrop));
    }
    result.InsertValue("processor", Processor->DebugJson());
    return result;
}

}   // namespace NKikimr::NArrow::NSSA

template <>
void Out<NKikimr::NArrow::NSSA::TColumnChainInfo>(IOutputStream& out, TTypeTraits<NKikimr::NArrow::NSSA::TColumnChainInfo>::TFuncParam item) {
    out << (ui64)item.GetColumnId();
}
