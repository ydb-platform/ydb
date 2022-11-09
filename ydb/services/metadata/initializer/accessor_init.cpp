#include "accessor_init.h"
#include "controller.h"

#include <ydb/core/grpc_services/local_rpc/local_rpc.h>

namespace NKikimr::NMetadataInitializer {

void TDSAccessorInitialized::Bootstrap() {
    RegisterState();
    Controller = std::make_shared<TInitializerController>(SelfId());
    Send(SelfId(), new TEvInitializerPreparationStart);
}

void TDSAccessorInitialized::Handle(TEvInitializerPreparationStart::TPtr& /*ev*/) {
    Prepare(Controller);
}

void TDSAccessorInitialized::Handle(TEvInitializerPreparationFinished::TPtr& ev) {
    auto modifiers = ev->Get()->GetModifiers();
    for (auto&& i : modifiers) {
        Modifiers.emplace_back(i);
    }
    if (Modifiers.size()) {
        ALS_INFO(NKikimrServices::METADATA_INITIALIZER) << "modifiers count: " << Modifiers.size();
        Modifiers.front()->Execute(SelfId(), Config);
    } else {
        ALS_INFO(NKikimrServices::METADATA_INITIALIZER) << "initialization finished";
        OnInitialized();
    }
}

void TDSAccessorInitialized::Handle(TEvInitializerPreparationProblem::TPtr& ev) {
    ALS_ERROR(NKikimrServices::METADATA_INITIALIZER) << "preparation problems: " << ev->Get()->GetErrorMessage();
    Schedule(TDuration::Seconds(1), new TEvInitializerPreparationStart);
}

void TDSAccessorInitialized::Handle(NInternal::NRequest::TEvRequestFinished::TPtr& /*ev*/) {
    ALS_INFO(NKikimrServices::METADATA_INITIALIZER) << "modifiers count: " << Modifiers.size();
    if (Modifiers.empty()) {
        return;
    }
    Modifiers.pop_front();
    if (Modifiers.size()) {
        Modifiers.front()->Execute(SelfId(), Config);
    } else {
        ALS_INFO(NKikimrServices::METADATA_INITIALIZER) << "initialization finished";
        OnInitialized();
    }
}

TDSAccessorInitialized::TDSAccessorInitialized(const NInternal::NRequest::TConfig& config)
    : Config(config)
{
}

}
