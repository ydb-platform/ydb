#include "debug_info.h"

#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

namespace NFq {

TString TDebugItem::ToString() const {
    TString result;
    result += "Query: " + Query + "\n";
    result += "Plan: " + Plan + "\n";
    result += "Ast: " + Ast + "\n";
    for (const auto& param: Params.GetValues()) {
        result += "Params: " + param.first + ", " + param.second.GetType().ToString() + "\n";
    }
    result += "Error: " + Error + "\n";
    return result;
}

size_t TDebugItem::GetByteSize() const {
    size_t paramsSize = 0;
    for (const auto& [key, value]: Params.GetValues()) {
        paramsSize += key.Size() + NYdb::TProtoAccessor::GetProto(value).ByteSizeLong();
    }
    return sizeof(*this)
            + Query.Size()
            + paramsSize
            + Plan.Size()
            + Ast.Size()
            + Error.Size();
}

} // namespace NFq