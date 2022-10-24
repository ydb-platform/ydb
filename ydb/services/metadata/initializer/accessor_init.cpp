#include "accessor_init.h"
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>

namespace NKikimr::NMetadataProvider {

void TDSAccessorInitialized::Bootstrap() {
    RegisterState();
    Modifiers.front()->Execute(SelfId(), Config.GetRequestConfig());
}

void TDSAccessorInitialized::Handle(NInternal::NRequest::TEvRequestFinished::TPtr& /*ev*/) {
    if (Modifiers.empty()) {
        return;
    }
    Modifiers.pop_front();
    if (Modifiers.size()) {
        Modifiers.front()->Execute(SelfId(), Config.GetRequestConfig());
    } else {
        OnInitialized();
    }
}

TDSAccessorInitialized::TDSAccessorInitialized(const TConfig& config, const TVector<ITableModifier::TPtr>& modifiers)
    : Config(config)
{
    Y_VERIFY(modifiers.size());
    for (auto&& i : modifiers) {
        Modifiers.emplace_back(i);
    }
}

}
