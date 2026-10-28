#pragma once

#include "yson_struct.h"

#include <util/generic/hash.h>

#include <string>

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

inline constexpr char DefaultOverridesFieldName[] = "overrides";

template <
    class TValue,
    class TKey = std::string,
    const char* OverridesFieldName = DefaultOverridesFieldName>
class TCustomizedConfig
    : public TYsonStructLite
{
public:
    const TValue& Get(const TKey& key) const;

    REGISTER_YSON_STRUCT_LITE(TCustomizedConfig);

private:
    TValue Default_;
    THashMap<TKey, TValue> Overrides_;

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree

#define CUSTOMIZED_CONFIG_INL_H_
#include "customized_config-inl.h"
#undef CUSTOMIZED_CONFIG_INL_H_
