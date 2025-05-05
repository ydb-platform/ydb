#include "abstract.h"
#include "collection.h"

#include <ydb/core/formats/arrow/accessor/composite/accessor.h>

#include <util/string/join.h>

namespace NKikimr::NArrow::NSSA {

NJson::TJsonValue IResourceProcessor::DebugJson() const {
    NJson::TJsonValue result = NJson::JSON_MAP;
    if (Input.size()) {
        result.InsertValue("i", JoinSeq(",", Input));
    }
    if (Output.size()) {
        result.InsertValue("o", JoinSeq(",", Output));
    }
    result.InsertValue("t", ::ToString(ProcessorType));
    auto internalJson = DoDebugJson();
    if (!internalJson.IsMap() || internalJson.GetMapSafe().size()) {
        result.InsertValue("p", std::move(internalJson));
    }
    return result;
}

TConclusion<IResourceProcessor::EExecutionResult> IResourceProcessor::Execute(const TProcessorContext& context, const TExecutionNodeContext& nodeContext) const {
    return DoExecute(context, nodeContext);
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
