#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/params/params.h>

#include <util/generic/string.h>

namespace NFq {

struct TDebugItem {
    TString Query;
    NYdb::TParams Params;
    TString Plan;
    TString Ast;
    TString Error;

    TString ToString() const;
    size_t GetByteSize() const;
};

using TDebugInfo = TVector<TDebugItem>;
using TDebugInfoPtr = std::shared_ptr<TDebugInfo>;

}   // namespace NFq
