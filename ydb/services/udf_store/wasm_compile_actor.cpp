#include "wasm_compile_actor.h"

#include "blob_chunks.h"
#include "metadata_subscription/udf_module.h"
#include "metadata_subscription/wasm_artifact.h"
#include "wasm/compile.h"
#include "wasm/manifest.h"
#include "wasm/registry_helpers.h"

#include <ydb/library/aclib/aclib.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/services/metadata/request/request_actor_cb.h>

namespace NKikimr::NUdfStore {

void TWasmCompileActor::Bootstrap() {
    Become(&TWasmCompileActor::StateMain);
    ModuleKind_ = WasmArtifactKindToString(EWasmArtifactKind::Module);
    ExecuteQuery(NTableQuery::BuildSelectModuleByNameQuery(ModulesTablePath_), true);
}

void TWasmCompileActor::ExecuteQuery(const TString& yql, bool readOnly) {
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
        case EStep::ReadModuleSource:
            NTableQuery::SetSelectModuleByNameParams(
                request,
                Name_,
                TUdfModule::TypeToString(EUdfType::WASM));
            break;
        case EStep::MarkCompiling:
            NTableQuery::SetUpdateCompileStatusParams(
                request,
                Name_,
                TUdfModule::TypeToString(EUdfType::WASM),
                ModuleSource_.Uid,
                TUdfModule::CompileStatusToString(ECompileStatus::Compiling),
                "");
            break;
        case EStep::ReadModuleChunks:
            NTableQuery::SetSelectSourceChunksParams(request, ModuleSource_.Uid);
            break;
        case EStep::ReadLibraryArtifact:
            NTableQuery::SetSelectArtifactParams(
                request,
                PendingLibraryName_,
                WasmArtifactKindToString(EWasmArtifactKind::Library),
                PendingLibraryUid_);
            break;
        case EStep::DeleteArtifactChunks:
            NTableQuery::SetDeleteArtifactChunksParams(request, Name_, ModuleKind_, ModuleSource_.Uid);
            break;
        case EStep::UpsertModuleArtifact:
            NTableQuery::SetUpsertArtifactParams(request, ArtifactRow_);
            break;
        case EStep::WriteArtifactChunk: {
            Y_ABORT_UNLESS(NextChunkWriteIndex_ < PendingChunkWrites_.size());
            const auto& chunk = PendingChunkWrites_[NextChunkWriteIndex_];
            NTableQuery::SetUpsertArtifactChunkParams(
                request,
                Name_,
                ModuleKind_,
                ModuleSource_.Uid,
                chunk.BlobKind,
                chunk.ChunkIdx,
                chunk.Data);
            break;
        }
        case EStep::UpdateMetaReady:
            NTableQuery::SetUpdateCompileStatusParams(
                request,
                Name_,
                TUdfModule::TypeToString(EUdfType::WASM),
                ModuleSource_.Uid,
                TUdfModule::CompileStatusToString(ECompileStatus::Ready),
                "");
            break;
        case EStep::VerifyStillCurrent:
        case EStep::ConfirmStillCurrent:
            NTableQuery::SetSelectModuleByNameParams(
                request,
                Name_,
                TUdfModule::TypeToString(EUdfType::WASM));
            break;
        case EStep::DeleteStaleArtifactChunks:
        case EStep::DeleteStaleArtifacts:
            NTableQuery::SetDeleteStaleArtifactsParams(
                request,
                Name_,
                ModuleKind_,
                ModuleSource_.Uid,
                TUdfModule::TypeToString(EUdfType::WASM));
            break;
        case EStep::UpdateMetaFailed:
            NTableQuery::SetUpdateCompileStatusParams(
                request,
                Name_,
                TUdfModule::TypeToString(EUdfType::WASM),
                ModuleSource_.Uid,
                TUdfModule::CompileStatusToString(ECompileStatus::Failed),
                ErrorMessage_);
            break;
    }

    auto controller = std::make_shared<NMetadata::NRequest::TNaiveExternalController<NMetadata::NRequest::TDialogYQLRequest>>(SelfId());
    NMetadata::NRequest::TYQLRequestExecutor::Execute(std::move(request), NACLib::TUserToken("metadata@system", {}), controller);
}

void TWasmCompileActor::HandleQueryResult(
    NMetadata::NRequest::TEvRequestResult<NMetadata::NRequest::TDialogYQLRequest>::TPtr& ev)
{
    OnQuerySuccess(ev->Get()->GetResult());
}

void TWasmCompileActor::HandleQueryFailed(NMetadata::NRequest::TEvRequestFailed::TPtr& ev) {
    if (Step_ == EStep::DeleteStaleArtifactChunks || Step_ == EStep::DeleteStaleArtifacts) {
        // Leftover rows of replaced uploads are not worth failing over, but
        // still confirm we own the modules row before anyone loads us.
        ALS_WARN(NKikimrServices::METADATA_PROVIDER)
            << "TWasmCompileActor: failed to drop stale artifacts of name=" << Name_
            << ": " << ev->Get()->GetErrorMessage();
        Step_ = EStep::ConfirmStillCurrent;
        ExecuteQuery(NTableQuery::BuildSelectModuleByNameQuery(ModulesTablePath_), true);
        return;
    }
    ReplyError(TStringBuilder()
        << "YQL request failed at compile step " << static_cast<int>(Step_)
        << ": " << ev->Get()->GetErrorMessage());
}

void TWasmCompileActor::OnQuerySuccess(const Ydb::Table::ExecuteDataQueryResponse& response) {
    try {
        switch (Step_) {
            case EStep::ReadModuleSource: {
                if (!NTableQuery::ParseModuleSourceResponse(response, ModuleSource_)) {
                    ReplyError(TStringBuilder() << "WASM module row not found for name=" << Name_);
                    return;
                }
                ParsedManifest_ = NWasm::ParseManifest(Manifest_);
                // The row name is the module's identity, so it must be the name
                // the manifest declares; otherwise the artifact would be
                // published under a name the loader never looks up.
                if (ParsedManifest_.ModuleName != Name_) {
                    ReplyError(TStringBuilder()
                        << "WASM module row name=" << Name_
                        << " does not match manifest module_name=" << ParsedManifest_.ModuleName);
                    return;
                }
                Step_ = EStep::MarkCompiling;
                ExecuteQuery(NTableQuery::BuildUpdateCompileStatusQuery(ModulesTablePath_), false);
                return;
            }
            case EStep::MarkCompiling: {
                Step_ = EStep::ReadModuleChunks;
                ExecuteQuery(NTableQuery::BuildSelectSourceChunksQuery(ModuleChunksTablePath_), true);
                return;
            }
            case EStep::ReadModuleChunks: {
                TVector<TString> chunks;
                if (!NTableQuery::ParseSourceChunksResponse(response, chunks)) {
                    ReplyError(TStringBuilder() << "Failed to read module source chunks for name=" << Name_);
                    return;
                }
                TString joinError;
                if (!JoinAndVerifyBlobs(
                        chunks,
                        ModuleSource_.ChunkCount,
                        ModuleSource_.Size,
                        ModuleSource_.Md5,
                        ModuleSource_.Body,
                        joinError))
                {
                    ReplyError(TStringBuilder()
                        << "Module source is corrupted for name=" << Name_ << ": " << joinError);
                    return;
                }
                Step_ = EStep::ReadLibraryArtifact;
                StartNextLibrary();
                return;
            }
            case EStep::ReadLibraryArtifact: {
                NTableQuery::TWasmArtifactRow artifact;
                if (!NTableQuery::ParseArtifactResponse(response, artifact)
                    || artifact.ObjectCodeChunkCount == 0)
                {
                    ReplyDeferred(TStringBuilder()
                        << "Compiled library artifact not ready for '" << PendingLibraryName_ << "'");
                    return;
                }
                ++NextLibraryIndex_;
                Step_ = EStep::ReadLibraryArtifact;
                StartNextLibrary();
                return;
            }
            case EStep::DeleteArtifactChunks: {
                // Write all chunks first; publish artifact row only when data is complete.
                StartWriteChunks();
                return;
            }
            case EStep::WriteArtifactChunk: {
                ++NextChunkWriteIndex_;
                WriteNextChunk();
                return;
            }
            case EStep::UpsertModuleArtifact: {
                Step_ = EStep::UpdateMetaReady;
                ExecuteQuery(NTableQuery::BuildUpdateCompileStatusQuery(ModulesTablePath_), false);
                return;
            }
            case EStep::UpdateMetaReady: {
                // The status update is scoped by uid, so it silently does
                // nothing when the module was re-uploaded while this compile
                // was running. Read the row back to tell that case apart from
                // a real success before anyone loads the artifact.
                Step_ = EStep::VerifyStillCurrent;
                ExecuteQuery(NTableQuery::BuildSelectModuleByNameQuery(ModulesTablePath_), true);
                return;
            }
            case EStep::VerifyStillCurrent: {
                NTableQuery::TModuleSourceRow current;
                if (!NTableQuery::ParseModuleSourceResponse(response, current)) {
                    ReplyDeferred(TStringBuilder()
                        << "WASM module row disappeared while compiling name=" << Name_);
                    return;
                }
                if (current.Uid != ModuleSource_.Uid) {
                    // Our artifact was built from an upload nobody wants any
                    // more. Leave it alone: the compile of the current upload
                    // is the one entitled to clean up, and it will collect
                    // ours too.
                    ReplyDeferred(TStringBuilder()
                        << "WASM module name=" << Name_ << " was re-uploaded while compiling: uid="
                        << ModuleSource_.Uid << " is now " << current.Uid);
                    return;
                }
                // Our upload is the current one, so every other artifact of
                // this module belongs to an upload that has been replaced.
                // Chunks first: the reverse order would leave chunks nobody
                // can find if the actor dies in between. The delete itself is
                // gated on modules.uid still matching, so a re-upload that
                // lands after this read cannot be wiped by us.
                Step_ = EStep::DeleteStaleArtifactChunks;
                ExecuteQuery(
                    NTableQuery::BuildDeleteStaleArtifactChunksQuery(
                        ArtifactChunksTablePath_,
                        ModulesTablePath_),
                    false);
                return;
            }
            case EStep::DeleteStaleArtifactChunks: {
                Step_ = EStep::DeleteStaleArtifacts;
                ExecuteQuery(
                    NTableQuery::BuildDeleteStaleArtifactsQuery(
                        ArtifactTablePath_,
                        ModulesTablePath_),
                    false);
                return;
            }
            case EStep::DeleteStaleArtifacts: {
                // Deletes are gated, but ReplySuccess would still load our
                // artifact under a name whose modules.uid may already be
                // someone else's. Read the row once more before publishing.
                Step_ = EStep::ConfirmStillCurrent;
                ExecuteQuery(NTableQuery::BuildSelectModuleByNameQuery(ModulesTablePath_), true);
                return;
            }
            case EStep::ConfirmStillCurrent: {
                NTableQuery::TModuleSourceRow current;
                if (!NTableQuery::ParseModuleSourceResponse(response, current)) {
                    ReplyDeferred(TStringBuilder()
                        << "WASM module row disappeared while compiling name=" << Name_);
                    return;
                }
                if (current.Uid != ModuleSource_.Uid) {
                    ReplyDeferred(TStringBuilder()
                        << "WASM module name=" << Name_ << " was re-uploaded while compiling: uid="
                        << ModuleSource_.Uid << " is now " << current.Uid);
                    return;
                }
                ReplySuccess();
                return;
            }
            case EStep::UpdateMetaFailed:
                ReplyError(ErrorMessage_);
                return;
        }
    } catch (const std::exception& ex) {
        FailAndPersist(ex.what());
    }
}

void TWasmCompileActor::StartNextLibrary() {
    if (NextLibraryIndex_ >= ParsedManifest_.RequiredLibraries.size()) {
        CompileUserModule();
        return;
    }
    PendingLibraryName_ = ParsedManifest_.RequiredLibraries[NextLibraryIndex_];
    const auto* uid = LibraryUids_.FindPtr(PendingLibraryName_);
    if (!uid) {
        // The library was not in the snapshot this compile was queued from; a
        // later snapshot will re-queue it once it appears.
        ReplyDeferred(TStringBuilder()
            << "Library '" << PendingLibraryName_ << "' is not known yet");
        return;
    }
    PendingLibraryUid_ = *uid;
    ExecuteQuery(NTableQuery::BuildSelectArtifactQuery(ArtifactTablePath_), true);
}

void TWasmCompileActor::ValidateExports() {
    const auto format = NWasm::DetectBytecodeFormat(ParsedManifest_.ModuleExtension);
    const auto exports = NWasm::CollectWasmExports(ModuleSource_.Body, format);

    auto requireExport = [&](const TString& exportName) -> const NWasm::TWasmExportSignature* {
        if (exportName.empty()) {
            return nullptr;
        }
        const auto* signature = exports.FindPtr(exportName);
        if (!signature) {
            ythrow yexception()
                << "Wasm module for UDF '" << Name_
                << "' does not export function '" << exportName << "'";
        }
        return signature;
    };

    // Every export goes through InvokeUdfExport, which passes the context, the
    // result pointer and each argument as UintPtr (i64 under the memory64
    // layout the engine forces) and expects nothing back. A module built for
    // another pointer width traps inside WAVM on the first row, so say so now.
    auto requireAbiTypes = [&](const NWasm::TWasmExportSignature& signature, const TString& exportName) {
        for (size_t i = 0; i < signature.ParamTypes.size(); ++i) {
            if (signature.ParamTypes[i] != NWasm::EWasmExportValueType::I64) {
                ythrow yexception()
                    << "Wasm export '" << exportName << "' for UDF '" << Name_
                    << "' takes " << NWasm::WasmExportValueTypeAsStr(signature.ParamTypes[i])
                    << " as parameter " << i << ", but every UDF export parameter must be i64";
            }
        }
        if (signature.ResultCount != 0) {
            ythrow yexception()
                << "Wasm export '" << exportName << "' for UDF '" << Name_
                << "' returns " << signature.ResultCount
                << " values, but a UDF export writes its result through the result pointer"
                   " and must return none";
        }
    };

    auto requireArity = [&](
        const NWasm::TWasmExportSignature& signature,
        const TString& exportName,
        size_t expectedParams,
        TStringBuf shape)
    {
        if (signature.ParamCount != expectedParams) {
            ythrow yexception()
                << "Wasm export '" << exportName << "' for UDF '" << Name_
                << "' has " << signature.ParamCount << " parameters, but needs "
                << expectedParams << " (" << shape << ")";
        }
    };

    for (const auto& descriptor : ParsedManifest_.Functions) {
        if (descriptor.Binding == NWasm::EWasmUdfBinding::TypeConfigCallable) {
            // create(ctx, result, typeConfig) and destroy(ctx, result, handle)
            // have a fixed shape; the call export's arity follows the method.
            if (const auto* createSignature = requireExport(descriptor.CreateExport)) {
                requireAbiTypes(*createSignature, descriptor.CreateExport);
                requireArity(*createSignature, descriptor.CreateExport, 3,
                    "context, result pointer, type config");
            }
            if (const auto* callSignature = requireExport(descriptor.CallExport)) {
                requireAbiTypes(*callSignature, descriptor.CallExport);
                // TWasmConfiguredCallable::Run passes the object handle ahead
                // of the method arguments, so the export takes three fixed
                // slots plus one per declared argument.
                requireArity(*callSignature, descriptor.CallExport,
                    3 + descriptor.Args.size(),
                    "context, result pointer, object handle, one slot per argument");
            }
            if (const auto* destroySignature = requireExport(descriptor.DestroyExport)) {
                requireAbiTypes(*destroySignature, descriptor.DestroyExport);
                requireArity(*destroySignature, descriptor.DestroyExport, 3,
                    "context, result pointer, object handle");
            }
            continue;
        }

        const TString exportName(NWasm::PlainWasmExport(descriptor));
        const auto* signature = requireExport(exportName);
        if (!signature) {
            continue;
        }
        requireAbiTypes(*signature, exportName);
        if (descriptor.CallingConvention != NWasm::EWasmCallingConvention::Bridge) {
            continue;
        }
        // A bridge export is called as (ctx, resultPtr, arg handles...) and
        // writes its result through resultPtr, so a mismatch here means the
        // manifest and the module disagree about the argument list. Catching
        // it at registration beats a WAVM type error on the first row.
        const size_t expectedParams = descriptor.ArgTypes.size() + 2;
        if (signature->ParamCount != expectedParams || signature->ResultCount != 0) {
            ythrow yexception()
                << "Wasm export '" << exportName << "' for UDF '" << Name_
                << "' has " << signature->ParamCount << " parameters and "
                << signature->ResultCount << " results, but calling_convention="
                << NWasm::CallingConventionAsStr(descriptor.CallingConvention)
                << " with " << descriptor.ArgTypes.size()
                << " declared arguments needs " << expectedParams
                << " parameters (context, result pointer, one per argument)"
                   " and no results";
        }
    }
}

void TWasmCompileActor::CompileUserModule() {
    try {
        ValidateExports();
        const auto format = NWasm::DetectBytecodeFormat(ParsedManifest_.ModuleExtension);
        const TString objectCode = NWasm::CompileModuleObjectCode(ModuleSource_.Body, format);
        const auto wasmChunks = SplitBlob(ModuleSource_.Body);
        const auto objectChunks = SplitBlob(objectCode);

        PendingChunkWrites_.clear();
        for (ui64 i = 0; i < wasmChunks.size(); ++i) {
            PendingChunkWrites_.push_back({
                .BlobKind = BlobKindWasmData(),
                .ChunkIdx = i,
                .Data = wasmChunks[i],
            });
        }
        for (ui64 i = 0; i < objectChunks.size(); ++i) {
            PendingChunkWrites_.push_back({
                .BlobKind = BlobKindObjectCode(),
                .ChunkIdx = i,
                .Data = objectChunks[i],
            });
        }

        ArtifactRow_ = NTableQuery::TWasmArtifactRow{
            .Id = Name_,
            .Kind = ModuleKind_,
            .Uid = ModuleSource_.Uid,
            .Version = ModuleSource_.Version,
            .Format = ParsedManifest_.ModuleExtension,
            .WasmDataSize = ModuleSource_.Body.size(),
            .WasmDataChunkCount = wasmChunks.size(),
            .ObjectCodeSize = objectCode.size(),
            .ObjectCodeChunkCount = objectChunks.size(),
        };

        Step_ = EStep::DeleteArtifactChunks;
        ExecuteQuery(NTableQuery::BuildDeleteArtifactChunksQuery(ArtifactChunksTablePath_), false);
    } catch (const std::exception& ex) {
        FailAndPersist(ex.what());
    }
}

void TWasmCompileActor::StartWriteChunks() {
    NextChunkWriteIndex_ = 0;
    WriteNextChunk();
}

void TWasmCompileActor::WriteNextChunk() {
    if (NextChunkWriteIndex_ >= PendingChunkWrites_.size()) {
        Step_ = EStep::UpsertModuleArtifact;
        ExecuteQuery(NTableQuery::BuildUpsertArtifactQuery(ArtifactTablePath_), false);
        return;
    }
    Step_ = EStep::WriteArtifactChunk;
    ExecuteQuery(NTableQuery::BuildUpsertArtifactChunkQuery(ArtifactChunksTablePath_), false);
}

void TWasmCompileActor::FailAndPersist(const TString& message) {
    ErrorMessage_ = message;
    if (ModuleSource_.Uid.empty()) {
        ReplyError(message);
        return;
    }
    Step_ = EStep::UpdateMetaFailed;
    ExecuteQuery(NTableQuery::BuildUpdateCompileStatusQuery(ModulesTablePath_), false);
}

void TWasmCompileActor::ReplyError(const TString& message) {
    ALS_ERROR(NKikimrServices::METADATA_PROVIDER) << "TWasmCompileActor: " << message;
    Send(ReplyTo_, new TEvWasmCompileResponse(false, Name_, message));
    PassAway();
}

void TWasmCompileActor::ReplyDeferred(const TString& reason) {
    ALS_INFO(NKikimrServices::METADATA_PROVIDER)
        << "TWasmCompileActor: deferred WASM UDF '" << Name_ << "': " << reason;
    Send(ReplyTo_, new TEvWasmCompileResponse(false, Name_, reason, true));
    PassAway();
}

void TWasmCompileActor::ReplySuccess() {
    ALS_INFO(NKikimrServices::METADATA_PROVIDER)
        << "TWasmCompileActor: compiled WASM UDF '" << Name_
        << "' for cpu_spec='" << CpuSpec_ << "'";
    Send(ReplyTo_, new TEvWasmCompileResponse(true, Name_));
    PassAway();
}

} // namespace NKikimr::NUdfStore
