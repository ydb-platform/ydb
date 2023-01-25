#pragma once

#include <util/generic/string.h>

#include <ydb/public/sdk/cpp/client/ydb_params/params.h>

namespace NYq {

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

} // namespace NYq