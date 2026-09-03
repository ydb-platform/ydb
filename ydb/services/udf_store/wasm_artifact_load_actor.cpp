#include "wasm_artifact_load_actor.h"

#include "blob_chunks.h"
#include "metadata_subscription/wasm_artifact.h"
#include "table_query.h"
#include "wasm/compile.h"
#include "wasm/manifest.h"
#include "wasm/registry_helpers.h"
#include "wasm/single_module_loader.h"
#include "wasm/udf_function.h"

#include <ydb/library/aclib/aclib.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/services/metadata/request/request_actor_cb.h>

#include <util/string/join.h>

#include <algorithm>

namespace NKikimr::NUdfStore {

void TWasmArtifactLoadActor::Bootstrap() {
    Become(&TWasmArtifactLoadActor::StateMain);
    try {
        ParsedManifest_ = NWasm::ParseManifest(Manifest_);
    } catch (const std::exception& ex) {
        ReplyError(TStringBuilder() << "Invalid manifest: " << ex.what());
        return;
    }
    // The artifact is stored under the module name, so a manifest declaring a
    // different name would have us read one module's artifact and register it
    // under another's name.
    if (ParsedManifest_.ModuleName != Name_) {
        ReplyError(TStringBuilder()
            << "Module name=" << Name_
            << " does not match manifest module_name=" << ParsedManifest_.ModuleName);
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
                Name_,
                WasmArtifactKindToString(EWasmArtifactKind::Module));
            break;
        case EStep::ReadModuleWasmChunks:
            NTableQuery::SetSelectArtifactChunksParams(
                request,
                Name_,
                WasmArtifactKindToString(EWasmArtifactKind::Module),
                BlobKindWasmData());
            break;
        case EStep::ReadModuleObjectChunks:
            NTableQuery::SetSelectArtifactChunksParams(
                request,
                Name_,
                WasmArtifactKindToString(EWasmArtifactKind::Module),
                BlobKindObjectCode());
            break;
        case EStep::ReadLibraryArtifact:
            NTableQuery::SetSelectArtifactParams(
                request,
                PendingLibraryName_,
                WasmArtifactKindToString(EWasmArtifactKind::Library));
            break;
        case EStep::ReadLibraryWasmChunks:
            NTableQuery::SetSelectArtifactChunksParams(
                request,
                PendingLibraryName_,
                WasmArtifactKindToString(EWasmArtifactKind::Library),
                BlobKindWasmData());
            break;
        case EStep::ReadLibraryObjectChunks:
            NTableQuery::SetSelectArtifactChunksParams(
                request,
                PendingLibraryName_,
                WasmArtifactKindToString(EWasmArtifactKind::Library),
                BlobKindObjectCode());
            break;
        case EStep::RegisterModule:
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
                || ModuleArtifact_.ObjectCodeChunkCount == 0)
            {
                ReplyError(TStringBuilder() << "Compiled module artifact not found for name=" << Name_);
                return;
            }
            Step_ = EStep::ReadModuleWasmChunks;
            ExecuteQuery(NTableQuery::BuildSelectArtifactChunksQuery(ArtifactChunksTablePath_), true);
            return;
        }
        case EStep::ReadModuleWasmChunks: {
            if (!NTableQuery::ParseArtifactChunksResponse(response, PendingWasmChunks_)) {
                ReplyError(TStringBuilder() << "Failed to read module wasm_data chunks for name=" << Name_);
                return;
            }
            if (PendingWasmChunks_.size() != ModuleArtifact_.WasmDataChunkCount) {
                ReplyError(TStringBuilder()
                    << "Module wasm_data chunk_count mismatch for name=" << Name_);
                return;
            }
            Step_ = EStep::ReadModuleObjectChunks;
            ExecuteQuery(NTableQuery::BuildSelectArtifactChunksQuery(ArtifactChunksTablePath_), true);
            return;
        }
        case EStep::ReadModuleObjectChunks: {
            TVector<TString> objectChunks;
            if (!NTableQuery::ParseArtifactChunksResponse(response, objectChunks)) {
                ReplyError(TStringBuilder() << "Failed to read module object_code chunks for name=" << Name_);
                return;
            }
            if (objectChunks.size() != ModuleArtifact_.ObjectCodeChunkCount) {
                ReplyError(TStringBuilder()
                    << "Module object_code chunk_count mismatch for name=" << Name_);
                return;
            }
            ModuleArtifact_.WasmData = JoinBlobs(PendingWasmChunks_);
            ModuleArtifact_.ObjectCode = JoinBlobs(objectChunks);
            PendingWasmChunks_.clear();
            Step_ = EStep::ReadLibraryArtifact;
            StartNextLibrary();
            return;
        }
        case EStep::ReadLibraryArtifact: {
            if (!NTableQuery::ParseArtifactResponse(response, PendingLibraryArtifact_)
                || PendingLibraryArtifact_.ObjectCodeChunkCount == 0)
            {
                ReplyError(TStringBuilder()
                    << "Compiled library artifact not found for '" << PendingLibraryName_ << "'");
                return;
            }
            Step_ = EStep::ReadLibraryWasmChunks;
            ExecuteQuery(NTableQuery::BuildSelectArtifactChunksQuery(ArtifactChunksTablePath_), true);
            return;
        }
        case EStep::ReadLibraryWasmChunks: {
            if (!NTableQuery::ParseArtifactChunksResponse(response, PendingWasmChunks_)) {
                ReplyError(TStringBuilder()
                    << "Failed to read library wasm_data chunks for '" << PendingLibraryName_ << "'");
                return;
            }
            if (PendingWasmChunks_.size() != PendingLibraryArtifact_.WasmDataChunkCount) {
                ReplyError(TStringBuilder()
                    << "Library wasm_data chunk_count mismatch for '" << PendingLibraryName_ << "'");
                return;
            }
            Step_ = EStep::ReadLibraryObjectChunks;
            ExecuteQuery(NTableQuery::BuildSelectArtifactChunksQuery(ArtifactChunksTablePath_), true);
            return;
        }
        case EStep::ReadLibraryObjectChunks: {
            TVector<TString> objectChunks;
            if (!NTableQuery::ParseArtifactChunksResponse(response, objectChunks)) {
                ReplyError(TStringBuilder()
                    << "Failed to read library object_code chunks for '" << PendingLibraryName_ << "'");
                return;
            }
            if (objectChunks.size() != PendingLibraryArtifact_.ObjectCodeChunkCount) {
                ReplyError(TStringBuilder()
                    << "Library object_code chunk_count mismatch for '" << PendingLibraryName_ << "'");
                return;
            }
            const auto format = PendingLibraryArtifact_.Format == "wat" || PendingLibraryArtifact_.Format == "wast"
                ? NYdb::NWasm::EBytecodeFormat::HumanReadable
                : NYdb::NWasm::EBytecodeFormat::Binary;
            Libraries_.push_back(NWasm::TNamedModuleBytecode{
                .Name = PendingLibraryName_,
                .Bytecode = NWasm::MakeModuleBytecode(
                    JoinBlobs(PendingWasmChunks_),
                    JoinBlobs(objectChunks),
                    format),
            });
            PendingWasmChunks_.clear();
            ++NextLibraryIndex_;
            Step_ = EStep::ReadLibraryArtifact;
            StartNextLibrary();
            return;
        }
        case EStep::RegisterModule:
            return;
    }
}

void TWasmArtifactLoadActor::StartNextLibrary() {
    if (NextLibraryIndex_ >= ParsedManifest_.RequiredLibraries.size()) {
        RegisterLoadedModule();
        return;
    }
    PendingLibraryName_ = ParsedManifest_.RequiredLibraries[NextLibraryIndex_];
    ExecuteQuery(NTableQuery::BuildSelectArtifactQuery(ArtifactTablePath_), true);
}

void TWasmArtifactLoadActor::RegisterLoadedModule() {
    const auto format = ModuleArtifact_.Format == "wat" || ModuleArtifact_.Format == "wast"
        ? NYdb::NWasm::EBytecodeFormat::HumanReadable
        : NYdb::NWasm::EBytecodeFormat::Binary;
    Step_ = EStep::RegisterModule;

    try {
        for (const auto& required : ParsedManifest_.RequiredLibraries) {
            const bool found = std::any_of(
                Libraries_.begin(),
                Libraries_.end(),
                [&](const NWasm::TNamedModuleBytecode& library) {
                    return library.Name == required && library.Bytecode.ObjectCode;
                });
            if (!found) {
                ReplyError(TStringBuilder()
                    << "Required library '" << required
                    << "' was not loaded before registering WASM UDF '" << Name_ << "'");
                return;
            }
        }

        NWasm::TWasmLoadParams params{
            .Manifest = ParsedManifest_,
            .ModuleWasmData = ModuleArtifact_.WasmData,
            .ModuleObjectCode = ModuleArtifact_.ObjectCode,
            .ModuleFormat = format,
            .Libraries = Libraries_,
        };
        auto state = NWasm::LoadWasmFromManifest(params);
        auto module = NWasm::BuildWasmSoModule(state);
        if (!module) {
            ReplyError("BuildWasmSoModule returned null");
            return;
        }
        // Unique path so multiple WASM modules can be registered in one registry.
        // Drop the previous body first: re-uploading a module keeps its name, so
        // AddModule would otherwise collide with the copy already registered.
        if (auto* dynamicRegistry = NKqp::AsDynamicFunctionRegistry(FunctionRegistry_.Get())) {
            dynamicRegistry->RemoveModule(Name_);
        }
        FunctionRegistry_->AddModule(
            TStringBuilder() << "wasm:" << Name_,
            Name_,
            std::move(module));
        ALS_INFO(NKikimrServices::METADATA_PROVIDER)
            << "TWasmArtifactLoadActor: registered wasm UDF '" << Name_
            << "' with libraries=[" << JoinSeq(",", ParsedManifest_.RequiredLibraries) << "]";
        Send(ReplyTo_, new TEvReadBodyResponse(true, Name_));
        PassAway();
    } catch (const std::exception& ex) {
        ReplyError(ex.what());
    }
}

void TWasmArtifactLoadActor::ReplyError(const TString& message) {
    ALS_ERROR(NKikimrServices::METADATA_PROVIDER)
        << "TWasmArtifactLoadActor: " << message;
    Send(ReplyTo_, new TEvReadBodyResponse(false, Name_, message));
    PassAway();
}

} // namespace NKikimr::NUdfStore
