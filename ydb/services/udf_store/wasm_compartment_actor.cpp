#include "wasm_compartment_actor.h"

#include "wasm/manifest.h"
#include "wasm/single_module_loader.h"

#include <ydb/library/actors/core/log.h>

namespace NKikimr::NUdfStore {

void TWasmCompartmentActor::Bootstrap() {
    Become(&TWasmCompartmentActor::StateMain);
}

void TWasmCompartmentActor::HandleLoad(TEvWasmCompartmentLoad::TPtr& ev) {
    const auto* msg = ev->Get();
    if (msg->Md5 != Md5_) {
        Send(ev->Sender, new TEvWasmCompartmentLoaded(
            false, Md5_, {}, "MD5 mismatch in compartment load request"));
        return;
    }

    try {
        auto manifest = NWasm::ParseManifest(msg->Manifest);
        NWasm::TWasmLoadParams params{
            .Md5 = msg->Md5,
            .Manifest = std::move(manifest),
            .ModuleWasmData = msg->ModuleWasmData,
            .ModuleObjectCode = msg->ModuleObjectCode,
            .ModuleFormat = msg->ModuleFormat,
            .Libraries = msg->Libraries,
        };
        State_ = NWasm::LoadWasmFromManifest(params);
        ALS_INFO(NKikimrServices::METADATA_PROVIDER)
            << "TWasmCompartmentActor: loaded wasm UDF '" << Md5_
            << "' module '" << State_->ModuleName << "'";
        Send(ev->Sender, new TEvWasmCompartmentLoaded(true, Md5_, State_));
    } catch (const std::exception& ex) {
        State_.reset();
        const TString error = TStringBuilder()
            << "Failed to load wasm compartment for UDF '" << Md5_ << "': " << ex.what();
        ALS_ERROR(NKikimrServices::METADATA_PROVIDER) << "TWasmCompartmentActor: " << error;
        Send(ev->Sender, new TEvWasmCompartmentLoaded(false, Md5_, {}, error));
    }
}

void TWasmCompartmentActor::HandleUnload(TEvWasmCompartmentUnload::TPtr& ev) {
    if (ev->Get()->Md5 == Md5_) {
        State_.reset();
        PassAway();
    }
}

} // namespace NKikimr::NUdfStore
