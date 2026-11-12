#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

struct TTableHint
    : public NYTree::TYsonStructLite
{
    bool RequireSyncReplica;
    bool PushDownGroupBy;

    REGISTER_YSON_STRUCT_LITE(TTableHint);

    static void Register(TRegistrar registrar);
};

//! Formats |hint| as a string literal of the |WITH HINT| clause.
void FormatValue(TStringBuilderBase* builder, const TTableHint& hint, TStringBuf spec);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
