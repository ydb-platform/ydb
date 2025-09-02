#pragma once

#include <yql/essentials/core/yql_data_provider.h>

namespace NYql {

struct TPgState : public TThrRefBase
{
    using TPtr = TIntrusivePtr<TPgState>;

    TTypeAnnotationContext* Types = nullptr;
};

TDataProviderInitializer GetPgDataProviderInitializer();

} // namespace NYql
