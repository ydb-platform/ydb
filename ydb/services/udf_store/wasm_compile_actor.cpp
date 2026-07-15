#include "wasm_compile_actor.h"

#include "metadata_subscription/udf_meta.h"
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
    ExecuteQuery(NTableQuery::BuildSelectWasmSourceQuery(WasmSourceTablePath_), true);
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
        case EStep::ReadWasmSource:
            NTableQuery::SetSelectWasmSourceParams(request, Md5_);
            break;
        case EStep::ReadLibraryArtifact:
            NTableQuery::SetSelectArtifactParams(
                request,
                PendingLibraryName_,
                WasmArtifactKindToString(EWasmArtifactKind::Library));
            break;
        case EStep::UpsertModuleArtifact: {
            NTableQuery::TWasmArtifactRow row{
                .Id = Md5_,
                .Kind = WasmArtifactKindToString(EWasmArtifactKind::Module),
                .SourceMd5 = WasmSource_.Md5,
                .Version = WasmSource_.Version,
                .Format = ParsedManifest_.ModuleExtension,
                .WasmData = WasmSource_.Body,
                .ObjectCode = ModuleObjectCode_,
            };
            NTableQuery::SetUpsertArtifactParams(request, row);
            break;
        }
        case EStep::UpdateMetaReady:
            NTableQuery::SetUpdateCompileStatusParams(
                request,
                Md5_,
                TUdfMeta::CompileStatusToString(ECompileStatus::Ready),
                "");
            break;
        case EStep::UpdateMetaFailed:
            NTableQuery::SetUpdateCompileStatusParams(
                request,
                Md5_,
                TUdfMeta::CompileStatusToString(ECompileStatus::Failed),
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
            case EStep::ReadWasmSource: {
                if (!NTableQuery::ParseWasmSourceResponse(response, WasmSource_)) {
                    ReplyError(TStringBuilder() << "WASM source row not found for md5=" << Md5_);
                    return;
                }
                ParsedManifest_ = NWasm::ParseManifest(Manifest_);
                Step_ = EStep::ReadLibraryArtifact;
                StartNextLibrary();
                return;
            }
            case EStep::ReadLibraryArtifact: {
                NTableQuery::TWasmArtifactRow artifact;
                if (!NTableQuery::ParseArtifactResponse(response, artifact)
                    || artifact.ObjectCode.empty())
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
            case EStep::UpsertModuleArtifact: {
                Step_ = EStep::UpdateMetaReady;
                ExecuteQuery(NTableQuery::BuildUpdateCompileStatusQuery(MetaTablePath_), false);
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
        ExecuteQuery(NTableQuery::BuildUpdateCompileStatusQuery(MetaTablePath_), false);
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
    const auto exports = NWasm::CollectWasmExports(WasmSource_.Body, format);
    for (const auto& descriptor : ParsedManifest_.Functions) {
        if (!exports.contains(descriptor.Name)) {
            ythrow yexception()
                << "Wasm module for UDF '" << Md5_
                << "' does not export function '" << descriptor.Name << "'";
        }
    }
}

void TWasmCompileActor::CompileUserModule() {
    try {
        ValidateExports();
        const auto format = NWasm::DetectBytecodeFormat(ParsedManifest_.ModuleExtension);
        ModuleObjectCode_ = NWasm::CompileModuleObjectCode(WasmSource_.Body, format);
        Step_ = EStep::UpsertModuleArtifact;
        ExecuteQuery(NTableQuery::BuildUpsertArtifactQuery(ArtifactTablePath_), false);
    } catch (const std::exception& ex) {
        ErrorMessage_ = ex.what();
        Step_ = EStep::UpdateMetaFailed;
        ExecuteQuery(NTableQuery::BuildUpdateCompileStatusQuery(MetaTablePath_), false);
    }
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
