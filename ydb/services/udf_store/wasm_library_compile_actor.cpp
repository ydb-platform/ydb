#include "wasm_library_compile_actor.h"

#include "metadata_subscription/library_source.h"
#include "metadata_subscription/wasm_artifact.h"
#include "wasm/compile.h"

#include <ydb/library/aclib/aclib.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/services/metadata/request/request_actor_cb.h>

namespace NKikimr::NUdfStore {

void TWasmLibraryCompileActor::Bootstrap() {
    Become(&TWasmLibraryCompileActor::StateMain);
    ExecuteQuery(NTableQuery::BuildSelectLibrarySourceQuery(LibrarySourceTablePath_), true);
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
            NTableQuery::SetSelectLibrarySourceParams(request, LibraryName_);
            break;
        case EStep::UpsertArtifact: {
            const auto format = LibrarySource_.Body.StartsWith("(")
                ? NYdb::NWasm::EBytecodeFormat::HumanReadable
                : NYdb::NWasm::EBytecodeFormat::Binary;
            NTableQuery::TWasmArtifactRow row{
                .Id = LibraryName_,
                .Kind = WasmArtifactKindToString(EWasmArtifactKind::Library),
                .SourceMd5 = LibrarySource_.Md5,
                .Version = LibrarySource_.Version,
                .Format = format == NYdb::NWasm::EBytecodeFormat::HumanReadable ? "wat" : "wasm",
                .WasmData = LibrarySource_.Body,
                .ObjectCode = NWasm::CompileModuleObjectCode(LibrarySource_.Body, format),
            };
            NTableQuery::SetUpsertArtifactParams(request, row);
            break;
        }
        case EStep::UpdateMetaReady:
            NTableQuery::SetUpdateLibraryCompileStatusParams(
                request,
                LibraryName_,
                TUdfMeta::CompileStatusToString(ECompileStatus::Ready),
                "");
            break;
        case EStep::UpdateMetaFailed:
            NTableQuery::SetUpdateLibraryCompileStatusParams(
                request,
                LibraryName_,
                TUdfMeta::CompileStatusToString(ECompileStatus::Failed),
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
                if (!NTableQuery::ParseLibrarySourceResponse(response, LibrarySource_)) {
                    ReplyError(TStringBuilder() << "Library source '" << LibraryName_ << "' not found");
                    return;
                }
                CompileLibrary();
                return;
            }
            case EStep::UpsertArtifact: {
                Step_ = EStep::UpdateMetaReady;
                ExecuteQuery(NTableQuery::BuildUpdateLibraryCompileStatusQuery(LibrarySourceTablePath_), false);
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
        ErrorMessage_ = ex.what();
        Step_ = EStep::UpdateMetaFailed;
        ExecuteQuery(NTableQuery::BuildUpdateLibraryCompileStatusQuery(LibrarySourceTablePath_), false);
    }
}

void TWasmLibraryCompileActor::CompileLibrary() {
    try {
        Step_ = EStep::UpsertArtifact;
        ExecuteQuery(NTableQuery::BuildUpsertArtifactQuery(ArtifactTablePath_), false);
    } catch (const std::exception& ex) {
        ErrorMessage_ = ex.what();
        Step_ = EStep::UpdateMetaFailed;
        ExecuteQuery(NTableQuery::BuildUpdateLibraryCompileStatusQuery(LibrarySourceTablePath_), false);
    }
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
