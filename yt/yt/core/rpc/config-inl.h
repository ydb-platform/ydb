#ifndef CONFIG_INL_H_
#error "Direct inclusion of this file is not allowed, include config.h"
// For the sake of sane code completion.
#include "config.h"
#endif

#include <any>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

template <class TConfig>
TIntrusivePtr<TConfig>* TProtocolMapConfigBase::MutableTypedConfig(TStringBuf protocol)
{
    auto it = ProtocolToEntry_.find(protocol);
    if (it == ProtocolToEntry_.end()) {
        it = ProtocolToEntry_.emplace(protocol, TProtocolEntry{
            .CurrentConfig = std::any(TIntrusivePtr<TConfig>()),
            .IsNull = [] (const std::any& config) {
                return std::any_cast<const TIntrusivePtr<TConfig>&>(config) == nullptr;
            },
        }).first;
    }
    return &std::any_cast<TIntrusivePtr<TConfig>&>(it->second.CurrentConfig);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
