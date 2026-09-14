#include "wasm_library_compile_actor.h"

#include "blob_chunks.h"
#include "metadata_subscription/udf_module.h"
#include "metadata_subscription/wasm_artifact.h"
#include "wasm/compile.h"

#include <ydb/library/aclib/aclib.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/services/metadata/request/request_actor_cb.h>

namespace NKikimr::NUdfStore {

void TWasmLibraryCompileActor::Bootstrap() {
    Become(&TWasmLibraryCompileActor::StateMain);
    Kind_ = WasmArtifactKindToString(EWasmArtifactKind::Library);
    ExecuteQuery(NTableQuery::BuildSelectModuleByNameQuery(ModulesTablePath_), true);
}

void TWasmLibraryCompileActor::ExecuteQuery(const TString& yql, bool readOnly) {
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
        case EStep::ReadLibrarySource:
            NTableQuery::SetSelectModuleByNameParams(request, LibraryName_);
            break;
        case EStep::MarkCompiling:
            NTableQuery::SetUpdateCompileStatusParams(
                request,
                LibrarySource_.Uid,
                TUdfModule::CompileStatusToString(ECompileStatus::Compiling),
                "");
            break;
        case EStep::ReadLibraryChunks:
            NTableQuery::SetSelectSourceChunksParams(request, LibrarySource_.Uid);
            break;
        case EStep::DeleteArtifactChunks:
            NTableQuery::SetDeleteArtifactChunksParams(request, LibraryName_, Kind_);
            break;
        case EStep::UpsertArtifact:
            NTableQuery::SetUpsertArtifactParams(request, ArtifactRow_);
            break;
        case EStep::WriteArtifactChunk: {
            Y_ABORT_UNLESS(NextChunkWriteIndex_ < PendingChunkWrites_.size());
            const auto& chunk = PendingChunkWrites_[NextChunkWriteIndex_];
            NTableQuery::SetUpsertArtifactChunkParams(
                request,
                LibraryName_,
                Kind_,
                chunk.BlobKind,
                chunk.ChunkIdx,
                chunk.Data);
            break;
        }
        case EStep::UpdateMetaReady:
            NTableQuery::SetUpdateCompileStatusParams(
                request,
                LibrarySource_.Uid,
                TUdfModule::CompileStatusToString(ECompileStatus::Ready),
                "");
            break;
        case EStep::UpdateMetaFailed:
            NTableQuery::SetUpdateCompileStatusParams(
                request,
                LibrarySource_.Uid,
                TUdfModule::CompileStatusToString(ECompileStatus::Failed),
                ErrorMessage_);
            break;
    }

    auto controller = std::make_shared<NMetadata::NRequest::TNaiveExternalController<NMetadata::NRequest::TDialogYQLRequest>>(SelfId());
    NMetadata::NRequest::TYQLRequestExecutor::Execute(std::move(request), NACLib::TUserToken("metadata@system", {}), controller);
}

void TWasmLibraryCompileActor::HandleQueryResult(
    NMetadata::NRequest::TEvRequestResult<NMetadata::NRequest::TDialogYQLRequest>::TPtr& ev)
{
    OnQuerySuccess(ev->Get()->GetResult());
}

void TWasmLibraryCompileActor::HandleQueryFailed(NMetadata::NRequest::TEvRequestFailed::TPtr& ev) {
    ReplyError(TStringBuilder()
        << "YQL request failed at library compile step " << static_cast<int>(Step_)
        << ": " << ev->Get()->GetErrorMessage());
}

void TWasmLibraryCompileActor::OnQuerySuccess(const Ydb::Table::ExecuteDataQueryResponse& response) {
    try {
        switch (Step_) {
            case EStep::ReadLibrarySource: {
                if (!NTableQuery::ParseModuleSourceResponse(response, LibrarySource_)) {
                    ReplyError(TStringBuilder() << "Library source '" << LibraryName_ << "' not found");
                    return;
                }
                Step_ = EStep::MarkCompiling;
                ExecuteQuery(NTableQuery::BuildUpdateCompileStatusQuery(ModulesTablePath_), false);
                return;
            }
            case EStep::MarkCompiling: {
                Step_ = EStep::ReadLibraryChunks;
                ExecuteQuery(NTableQuery::BuildSelectSourceChunksQuery(ModuleChunksTablePath_), true);
                return;
            }
            case EStep::ReadLibraryChunks: {
                TVector<TString> chunks;
                if (!NTableQuery::ParseSourceChunksResponse(response, chunks)) {
                    ReplyError(TStringBuilder()
                        << "Failed to read library source chunks for '" << LibraryName_ << "'");
                    return;
                }
                if (chunks.size() != LibrarySource_.ChunkCount) {
                    ReplyError(TStringBuilder()
                        << "Library '" << LibraryName_ << "' chunk_count mismatch: meta="
                        << LibrarySource_.ChunkCount << " actual=" << chunks.size());
                    return;
                }
                LibrarySource_.Body = JoinBlobs(chunks);
                if (LibrarySource_.Size != 0 && LibrarySource_.Body.size() != LibrarySource_.Size) {
                    ReplyError(TStringBuilder()
                        << "Library '" << LibraryName_ << "' size mismatch: meta="
                        << LibrarySource_.Size << " actual=" << LibrarySource_.Body.size());
                    return;
                }
                CompileLibrary();
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
            case EStep::UpsertArtifact: {
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

void TWasmLibraryCompileActor::CompileLibrary() {
    try {
        const auto format = NWasm::DetectBytecodeFormatFromBody(LibrarySource_.Body);
        Format_ = format == NYdb::NWasm::EBytecodeFormat::HumanReadable ? "wat" : "wasm";
        const TString objectCode = NWasm::CompileModuleObjectCode(LibrarySource_.Body, format);
        const auto wasmChunks = SplitBlob(LibrarySource_.Body);
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
            .Id = LibraryName_,
            .Kind = Kind_,
            .SourceMd5 = LibrarySource_.Md5,
            .Version = LibrarySource_.Version,
            .Format = Format_,
            .WasmDataSize = LibrarySource_.Body.size(),
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

void TWasmLibraryCompileActor::StartWriteChunks() {
    NextChunkWriteIndex_ = 0;
    WriteNextChunk();
}

void TWasmLibraryCompileActor::WriteNextChunk() {
    if (NextChunkWriteIndex_ >= PendingChunkWrites_.size()) {
        Step_ = EStep::UpsertArtifact;
        ExecuteQuery(NTableQuery::BuildUpsertArtifactQuery(ArtifactTablePath_), false);
        return;
    }
    Step_ = EStep::WriteArtifactChunk;
    ExecuteQuery(NTableQuery::BuildUpsertArtifactChunkQuery(ArtifactChunksTablePath_), false);
}

void TWasmLibraryCompileActor::FailAndPersist(const TString& message) {
    ErrorMessage_ = message;
    if (LibrarySource_.Uid.empty()) {
        ReplyError(message);
        return;
    }
    Step_ = EStep::UpdateMetaFailed;
    ExecuteQuery(NTableQuery::BuildUpdateCompileStatusQuery(ModulesTablePath_), false);
}

void TWasmLibraryCompileActor::ReplyError(const TString& message) {
    ALS_ERROR(NKikimrServices::METADATA_PROVIDER) << "TWasmLibraryCompileActor: " << message;
    Send(ReplyTo_, new TEvLibraryCompileResponse(false, LibraryName_, message));
    PassAway();
}

void TWasmLibraryCompileActor::ReplySuccess() {
    ALS_INFO(NKikimrServices::METADATA_PROVIDER)
        << "TWasmLibraryCompileActor: compiled library '" << LibraryName_
        << "' for cpu_spec='" << CpuSpec_ << "'";
    Send(ReplyTo_, new TEvLibraryCompileResponse(true, LibraryName_));
    PassAway();
}

} // namespace NKikimr::NUdfStore
