#ifndef CONFIG_INL_H_
#error "Direct inclusion of this file is not allowed, include config.h"
// For the sake of sane code completion.
#include "config.h"
#endif

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

template <class TTypedConfigPtr>
NYTree::IMapNodePtr TLogWriterConfig::BuildFullConfig(const TTypedConfigPtr& typedConfig)
{
    auto result = NYTree::GetEphemeralNodeFactory()->CreateMap();
    for (const auto& [key, value] : NYTree::ConvertToNode(this)->AsMap()->GetChildren()) {
        result->AddChild(key, value);
    }
    for (const auto& [key, value] : NYTree::ConvertToNode(typedConfig)->AsMap()->GetChildren()) {
        result->AddChild(key, value);
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
