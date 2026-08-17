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
    ExecuteQuery(NTableQuery::BuildSelectModuleByMd5Query(ModulesTablePath_), true);
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
            NTableQuery::SetSelectModuleByMd5Params(request, Md5_);
            break;
        case EStep::MarkCompiling:
            NTableQuery::SetUpdateCompileStatusParams(
                request,
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
                WasmArtifactKindToString(EWasmArtifactKind::Library));
            break;
        case EStep::DeleteArtifactChunks:
            NTableQuery::SetDeleteArtifactChunksParams(request, Md5_, ModuleKind_);
            break;
        case EStep::UpsertModuleArtifact:
            NTableQuery::SetUpsertArtifactParams(request, ArtifactRow_);
            break;
        case EStep::WriteArtifactChunk: {
            Y_ABORT_UNLESS(NextChunkWriteIndex_ < PendingChunkWrites_.size());
            const auto& chunk = PendingChunkWrites_[NextChunkWriteIndex_];
            NTableQuery::SetUpsertArtifactChunkParams(
                request,
                Md5_,
                ModuleKind_,
                chunk.BlobKind,
                chunk.ChunkIdx,
                chunk.Data);
            break;
        }
        case EStep::UpdateMetaReady:
            NTableQuery::SetUpdateCompileStatusParams(
                request,
                ModuleSource_.Uid,
                TUdfModule::CompileStatusToString(ECompileStatus::Ready),
                "");
            break;
        case EStep::UpdateMetaFailed:
            NTableQuery::SetUpdateCompileStatusParams(
                request,
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
    ReplyError(TStringBuilder()
        << "YQL request failed at compile step " << static_cast<int>(Step_)
        << ": " << ev->Get()->GetErrorMessage());
}

void TWasmCompileActor::OnQuerySuccess(const Ydb::Table::ExecuteDataQueryResponse& response) {
    try {
        switch (Step_) {
            case EStep::ReadModuleSource: {
                if (!NTableQuery::ParseModuleSourceResponse(response, ModuleSource_)) {
                    ReplyError(TStringBuilder() << "WASM module row not found for md5=" << Md5_);
                    return;
                }
                ParsedManifest_ = NWasm::ParseManifest(Manifest_);
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
                    ReplyError(TStringBuilder() << "Failed to read module source chunks for md5=" << Md5_);
                    return;
                }
                if (chunks.size() != ModuleSource_.ChunkCount) {
                    ReplyError(TStringBuilder()
                        << "Module source chunk_count mismatch for md5=" << Md5_
                        << ": meta=" << ModuleSource_.ChunkCount << " actual=" << chunks.size());
                    return;
                }
                ModuleSource_.Body = JoinBlobs(chunks);
                if (ModuleSource_.Size != 0 && ModuleSource_.Body.size() != ModuleSource_.Size) {
                    ReplyError(TStringBuilder()
                        << "Module source size mismatch for md5=" << Md5_
                        << ": meta=" << ModuleSource_.Size << " actual=" << ModuleSource_.Body.size());
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
            case EStep::UpdateMetaReady:
                ReplySuccess();
                return;
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
    ExecuteQuery(NTableQuery::BuildSelectArtifactQuery(ArtifactTablePath_), true);
}

void TWasmCompileActor::ValidateExports() {
    const auto format = NWasm::DetectBytecodeFormat(ParsedManifest_.ModuleExtension);
    const auto exports = NWasm::CollectWasmExports(ModuleSource_.Body, format);

    auto requireExport = [&](const TString& exportName) {
        if (exportName.empty()) {
            return;
        }
        if (!exports.contains(exportName)) {
            ythrow yexception()
                << "Wasm module for UDF '" << Md5_
                << "' does not export function '" << exportName << "'";
        }
    };

    for (const auto& descriptor : ParsedManifest_.Functions) {
        if (descriptor.Binding == NWasm::EWasmUdfBinding::TypeConfigCallable) {
            requireExport(descriptor.CreateExport);
            requireExport(descriptor.CallExport);
            requireExport(descriptor.DestroyExport);
        } else {
            requireExport(TString(NWasm::PlainWasmExport(descriptor)));
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
            .Id = Md5_,
            .Kind = ModuleKind_,
            .SourceMd5 = ModuleSource_.Md5,
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
    Send(ReplyTo_, new TEvWasmCompileResponse(false, Md5_, message));
    PassAway();
}

void TWasmCompileActor::ReplyDeferred(const TString& reason) {
    ALS_INFO(NKikimrServices::METADATA_PROVIDER)
        << "TWasmCompileActor: deferred WASM UDF '" << Md5_ << "': " << reason;
    Send(ReplyTo_, new TEvWasmCompileResponse(false, Md5_, reason, true));
    PassAway();
}

void TWasmCompileActor::ReplySuccess() {
    ALS_INFO(NKikimrServices::METADATA_PROVIDER)
        << "TWasmCompileActor: compiled WASM UDF '" << Md5_
        << "' for cpu_spec='" << CpuSpec_ << "'";
    Send(ReplyTo_, new TEvWasmCompileResponse(true, Md5_));
    PassAway();
}

} // namespace NKikimr::NUdfStore
