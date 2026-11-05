#ifndef CUSTOMIZED_CONFIG_INL_H_
#error "Direct inclusion of this file is not allowed, include customized_config.h"
#include "customized_config.h"
#endif

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

template <class TValue, class TKey, const char* OverridesFieldName>
void TCustomizedConfig<TValue, TKey, OverridesFieldName>::Register(TRegistrar registrar)
{
    registrar.Parameter("default", &TThis::Default_)
        .Default();
    registrar.Parameter(OverridesFieldName, &TThis::Overrides_)
        .Default();
}

template <class TValue, class TKey, const char* OverridesFieldName>
const TValue& TCustomizedConfig<TValue, TKey, OverridesFieldName>::Get(const TKey& key) const
{
    if (auto overridesIt = Overrides_.find(key); overridesIt != Overrides_.end()) {
        return overridesIt->second;
    }

    return Default_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
