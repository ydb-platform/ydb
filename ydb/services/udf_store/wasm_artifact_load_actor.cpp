#include "wasm_artifact_load_actor.h"

#include "metadata_subscription/wasm_artifact.h"
#include "table_query.h"
#include "wasm/compile.h"
#include "wasm/manifest.h"
#include "wasm/registry_helpers.h"
#include "wasm/udf_function.h"

#include <ydb/library/aclib/aclib.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/services/metadata/request/request_actor_cb.h>

namespace NKikimr::NUdfStore {

void TWasmArtifactLoadActor::Bootstrap() {
    Become(&TWasmArtifactLoadActor::StateMain);
    try {
        ParsedManifest_ = NWasm::ParseManifest(Manifest_);
    } catch (const std::exception& ex) {
        ReplyError(TStringBuilder() << "Invalid manifest: " << ex.what());
        return;
    }
    ExecuteQuery(NTableQuery::BuildSelectArtifactQuery(ArtifactTablePath_), true);
}

void TWasmArtifactLoadActor::ExecuteQuery(const TString& yql, bool readOnly) {
    auto request = NMetadata::NRequest::TDialogYQLRequest::TRequest();
    request.mutable_query()->set_yql_text(yql);
    request.mutable_query_cache_policy()->set_keep_in_cache(true);
    if (readOnly) {
        request.mutable_tx_control()->mutable_begin_tx()->mutable_snapshot_read_only();
    } else {
        request.mutable_tx_control()->mutable_begin_tx()->mutable_serializable_read_write();
        request.mutable_tx_control()->set_commit_tx(true);
    }

    switch (Step_) {
        case EStep::ReadModuleArtifact:
            NTableQuery::SetSelectArtifactParams(
                request,
                Md5_,
                WasmArtifactKindToString(EWasmArtifactKind::Module));
            break;
        case EStep::ReadLibraryArtifact:
            NTableQuery::SetSelectArtifactParams(
                request,
                PendingLibraryName_,
                WasmArtifactKindToString(EWasmArtifactKind::Library));
            break;
        case EStep::LoadCompartment:
            return;
    }

    auto controller = std::make_shared<NMetadata::NRequest::TNaiveExternalController<NMetadata::NRequest::TDialogYQLRequest>>(SelfId());
    NMetadata::NRequest::TYQLRequestExecutor::Execute(std::move(request), NACLib::TUserToken("metadata@system", {}), controller);
}

void TWasmArtifactLoadActor::HandleQueryResult(
    NMetadata::NRequest::TEvRequestResult<NMetadata::NRequest::TDialogYQLRequest>::TPtr& ev)
{
    OnQuerySuccess(ev->Get()->GetResult());
}

void TWasmArtifactLoadActor::HandleQueryFailed(NMetadata::NRequest::TEvRequestFailed::TPtr& ev) {
    ReplyError(TStringBuilder()
        << "YQL request failed at load step " << static_cast<int>(Step_)
        << ": " << ev->Get()->GetErrorMessage());
}

void TWasmArtifactLoadActor::OnQuerySuccess(const Ydb::Table::ExecuteDataQueryResponse& response) {
    switch (Step_) {
        case EStep::ReadModuleArtifact: {
            if (!NTableQuery::ParseArtifactResponse(response, ModuleArtifact_)
                || ModuleArtifact_.ObjectCode.empty())
            {
                ReplyError(TStringBuilder() << "Compiled module artifact not found for md5=" << Md5_);
                return;
            }
            Step_ = EStep::ReadLibraryArtifact;
            StartNextLibrary();
            return;
        }
        case EStep::ReadLibraryArtifact: {
            NTableQuery::TWasmArtifactRow artifact;
            if (!NTableQuery::ParseArtifactResponse(response, artifact)
                || artifact.ObjectCode.empty())
            {
                ReplyError(TStringBuilder()
                    << "Compiled library artifact not found for '" << PendingLibraryName_ << "'");
                return;
            }
            const auto format = artifact.Format == "wat" || artifact.Format == "wast"
                ? NYdb::NWasm::EBytecodeFormat::HumanReadable
                : NYdb::NWasm::EBytecodeFormat::Binary;
            Libraries_.push_back(NWasm::TNamedModuleBytecode{
                .Name = PendingLibraryName_,
                .Bytecode = NWasm::MakeModuleBytecode(
                    artifact.WasmData,
                    artifact.ObjectCode,
                    format),
            });
            ++NextLibraryIndex_;
            StartNextLibrary();
            return;
        }
        case EStep::LoadCompartment:
            return;
    }
}

void TWasmArtifactLoadActor::StartNextLibrary() {
    if (NextLibraryIndex_ >= ParsedManifest_.RequiredLibraries.size()) {
        StartCompartmentLoad();
        return;
    }
    PendingLibraryName_ = ParsedManifest_.RequiredLibraries[NextLibraryIndex_];
    ExecuteQuery(NTableQuery::BuildSelectArtifactQuery(ArtifactTablePath_), true);
}

void TWasmArtifactLoadActor::StartCompartmentLoad() {
    const auto format = ModuleArtifact_.Format == "wat" || ModuleArtifact_.Format == "wast"
        ? NYdb::NWasm::EBytecodeFormat::HumanReadable
        : NYdb::NWasm::EBytecodeFormat::Binary;
    Step_ = EStep::LoadCompartment;
    Send(CompartmentActorId_, new TEvWasmCompartmentLoad(
        Md5_,
        Manifest_,
        ModuleArtifact_.WasmData,
        ModuleArtifact_.ObjectCode,
        format,
        Libraries_));
}

void TWasmArtifactLoadActor::HandleCompartmentLoaded(TEvWasmCompartmentLoaded::TPtr& ev) {
    if (ev->Get()->Md5 != Md5_) {
        ReplyError("Unexpected compartment loaded response MD5");
        return;
    }
    if (!ev->Get()->Success || !ev->Get()->State) {
        ReplyError(ev->Get()->ErrorMessage.empty()
            ? "Wasm compartment load failed"
            : ev->Get()->ErrorMessage);
        return;
    }

    try {
        auto module = NWasm::BuildWasmSoModule(ev->Get()->State);
        const TString moduleName = ev->Get()->State->ModuleName;
        FunctionRegistry_->AddModule(Md5_, moduleName, std::move(module));
        ALS_INFO(NKikimrServices::METADATA_PROVIDER)
            << "TWasmArtifactLoadActor: registered wasm module '" << moduleName
            << "' for UDF '" << Md5_ << "'";
        Send(ReplyTo_, new TEvReadBodyResponse(true, Md5_));
    } catch (const std::exception& ex) {
        ReplyError(TStringBuilder()
            << "Failed to register wasm UDF '" << Md5_ << "' in function registry: "
            << ex.what());
        return;
    }

    PassAway();
}

void TWasmArtifactLoadActor::ReplyError(const TString& message) {
    ALS_ERROR(NKikimrServices::METADATA_PROVIDER) << "TWasmArtifactLoadActor: " << message;
    Send(ReplyTo_, new TEvReadBodyResponse(false, Md5_, message));
    PassAway();
}

} // namespace NKikimr::NUdfStore
