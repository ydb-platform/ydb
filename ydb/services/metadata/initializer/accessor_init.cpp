#include "accessor_init.h"
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>

namespace NKikimr::NMetadataProvider {

void TDSAccessorInitialized::Bootstrap() {
    RegisterState();
    auto modifiers = BuildModifiers();
    for (auto&& i : modifiers) {
        Modifiers.emplace_back(i);
    }
    if (Modifiers.size()) {
        ALS_INFO(NKikimrServices::METADATA_PROVIDER) << "modifiers count: " << Modifiers.size();
        Modifiers.front()->Execute(SelfId(), Config);
    } else {
        ALS_INFO(NKikimrServices::METADATA_PROVIDER) << "initialization finished";
        OnInitialized();
    }
}

void TDSAccessorInitialized::Handle(NInternal::NRequest::TEvRequestFinished::TPtr& /*ev*/) {
    ALS_INFO(NKikimrServices::METADATA_PROVIDER) << "modifiers count: " << Modifiers.size();
    if (Modifiers.empty()) {
        return;
    }
    Modifiers.pop_front();
    if (Modifiers.size()) {
        Modifiers.front()->Execute(SelfId(), Config);
    } else {
        ALS_INFO(NKikimrServices::METADATA_PROVIDER) << "initialization finished";
        OnInitialized();
    }
}

TDSAccessorInitialized::TDSAccessorInitialized(const NInternal::NRequest::TConfig& config)
    : Config(config)
{
}

}
