#include "mutation_actor.h"
#include <ydb/public/lib/udf/manifest/manifest.h>
#include <ydb/services/udf_store/wasm/compile.h>

#include "common.h"
#include "events.h"
#include "table_query.h"

#include <ydb/services/udf_store/blob_chunks.h>
#include <ydb/services/udf_store/metadata_subscription/storage_paths.h>
#include <ydb/services/metadata/request/common.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

#include <library/cpp/digest/md5/md5.h>
#include <library/cpp/json/json_reader.h>

#include <util/generic/guid.h>
#include <util/string/builder.h>
#include <util/string/strip.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::GRPC_SERVER

namespace NKikimr::NUdfApi {

using namespace NActors;

using NUdfStore::EUdfType;

namespace {

using TEvYqlResult = NMetadata::NRequest::TEvRequestResult<NMetadata::NRequest::TDialogYQLRequest>;

class TUploadModuleActor: public TActorBootstrapped<TUploadModuleActor> {
    enum class EStep {
        UpsertChunk,
        FlipModule,
        CleanupChunks,
    };

public:
    TUploadModuleActor(const TActorId& replyTo, const Ydb::Udf::UploadModuleParams& params, TString body)
        : ReplyTo_(replyTo)
        , Params_(params)
        , Body_(std::move(body))
    {
    }

    void Bootstrap() {
        Become(&TUploadModuleActor::StateMain);

        if (!IsWasmUdfEnabled()) {
            ReplyError(Ydb::StatusIds::PRECONDITION_FAILED,
                "UDF store with WASM support is not enabled on this cluster");
            return;
        }

        Ydb::StatusIds::StatusCode status = Ydb::StatusIds::BAD_REQUEST;
        TString error;
        if (!ResolveRequest(status, error)) {
            ReplyError(status, error);
            return;
        }

        // Chunks go in first, under a uid nothing points at yet: they are
        // invisible until the modules row is flipped over to them, so the live
        // version stays whole even if this actor dies halfway through.
        Uid_ = CreateGuidAsString();
        WriteNextChunk();
    }

    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvYqlResult, Handle);
            hFunc(NMetadata::NRequest::TEvRequestFailed, Handle);
            default:
                break;
        }
    }

private:
    bool ResolveRequest(Ydb::StatusIds::StatusCode& status, TString& error) {
        status = Ydb::StatusIds::BAD_REQUEST;
        status = ValidateUpload(Params_, error);
        if (status != Ydb::StatusIds::SUCCESS) {
            return false;
        }
        const auto manifest = NYdb::NUdfManifest::Parse(Params_.manifest_json());
        Name_ = manifest.Name;
        Type_ = manifest.Type == NYdb::NUdfManifest::EModuleType::Module ? EUdfType::WASM : EUdfType::LIBRARY;
        try {
            NUdfStore::NWasm::ValidateModuleSource(Body_, NUdfStore::NWasm::DetectBytecodeFormat(manifest.Extension));
        } catch (const std::exception& ex) {
            status = Ydb::StatusIds::BAD_REQUEST;
            error = ex.what();
            return false;
        }

        Md5_ = MD5::Calc(Body_);
        if (!Params_.expected_md5().empty()) {
            TString expected = TString(Params_.expected_md5());
            expected.to_lower();
            if (expected != Md5_) {
                status = Ydb::StatusIds::PRECONDITION_FAILED;
                error = TStringBuilder()
                    << "expected_md5 " << Params_.expected_md5() << " does not match the uploaded body md5 " << Md5_;
                return false;
            }
        }

        Chunks_ = NUdfStore::SplitBlob(Body_);
        return true;
    }

    void Handle(TEvYqlResult::TPtr& ev) {
        switch (Step_) {
            case EStep::UpsertChunk:
                ++NextChunkIndex_;
                WriteNextChunk();
                return;
            case EStep::FlipModule:
                HandleFlipResult(ev->Get()->GetResult());
                return;
            case EStep::CleanupChunks:
                Finish();
                return;
        }
    }

    void Handle(NMetadata::NRequest::TEvRequestFailed::TPtr& ev) {
        if (Step_ == EStep::CleanupChunks) {
            YDB_LOG_WARN("UdfService: best-effort chunk cleanup",
                {"name", Name_},
                {"uid", CleanupUid_},
                {"skipped", ev->Get()->GetErrorMessage()});
            Finish();
            return;
        }
        // Everything written so far hangs off a uid the modules row does not
        // point at, so it is unreachable garbage rather than a broken module.
        FailAndCleanup(ev->Get()->GetStatus(), TStringBuilder() << "UDF store write failed at step " << static_cast<int>(Step_) << ": " << ev->Get()->GetErrorMessage(),
                       Uid_);
    }

    void HandleFlipResult(const Ydb::Table::ExecuteDataQueryResponse& response) {
        NQuery::TModulePreState preState;
        if (!NQuery::ParseModulePreStateResponse(response, preState)) {
            FailAndCleanup(Ydb::StatusIds::INTERNAL_ERROR,
                "UDF store did not report the state of the module row", Uid_);
            return;
        }
        if (!preState.Applied) {
            Ydb::StatusIds::StatusCode status = Ydb::StatusIds::INTERNAL_ERROR;
            TString error;
            ExplainRejectedWrite(preState, status, error);
            FailAndCleanup(status, error, Uid_);
            return;
        }

        ReplacedExisting_ = preState.Existed;
        // The row points at the new uid now, so the body of the previous one is
        // what nothing can reach any more.
        StartCleanup(preState.Existed ? preState.Uid : TString());
    }

    //! Names the precondition the flip transaction found broken. Every branch
    //! mirrors one conjunct of the query predicate, read off the same pre-state
    //! the predicate was evaluated on.
    void ExplainRejectedWrite(
        const NQuery::TModulePreState& preState,
        Ydb::StatusIds::StatusCode& status,
        TString& error) const {
        const auto writeMode = WriteMode();
        if (preState.Existed && writeMode == Ydb::Udf::CREATE_ONLY) {
            status = Ydb::StatusIds::ALREADY_EXISTS;
            error = TStringBuilder() << "module '" << Name_ << "' already exists";
            return;
        }
        if (!preState.Existed && writeMode == Ydb::Udf::REPLACE_ONLY) {
            status = Ydb::StatusIds::NOT_FOUND;
            error = TStringBuilder() << "module '" << Name_ << "' does not exist";
            return;
        }
        if (!Params_.expected_uid().empty()) {
            if (!preState.Existed) {
                status = Ydb::StatusIds::ABORTED;
                error = TStringBuilder()
                    << "expected_uid " << Params_.expected_uid()
                    << " given, but module '" << Name_ << "' does not exist";
                return;
            }
            if (preState.Uid != Params_.expected_uid()) {
                status = Ydb::StatusIds::ABORTED;
                error = TStringBuilder()
                    << "module '" << Name_ << "' has uid " << preState.Uid
                    << ", which is not the expected " << Params_.expected_uid();
                return;
            }
        }
        if (preState.Existed && (!preState.TypeKnown || preState.Type != Type_)) {
            status = Ydb::StatusIds::PRECONDITION_FAILED;
            error = TStringBuilder()
                << "module '" << Name_ << "' already exists as " << preState.TypeName
                << " and cannot be replaced by " << NUdfStore::TUdfModule::TypeToString(Type_)
                << "; delete it first";
            return;
        }
        status = Ydb::StatusIds::INTERNAL_ERROR;
        error = TStringBuilder() << "upload of '" << Name_ << "' was refused by the UDF store for no known reason";
    }

    Ydb::Udf::WriteMode WriteMode() const {
        return Params_.write_mode() == Ydb::Udf::WRITE_MODE_UNSPECIFIED
            ? Ydb::Udf::CREATE_OR_REPLACE
            : Params_.write_mode();
    }

    void WriteNextChunk() {
        if (NextChunkIndex_ < Chunks_.size()) {
            Step_ = EStep::UpsertChunk;
            ExecuteYqlAsSystem(
                SelfId(),
                NQuery::BuildUpsertSourceChunkQuery(NUdfStore::GetModuleChunksTablePath()),
                false,
                [this](Ydb::Table::ExecuteDataQueryRequest& request) {
                    NQuery::SetUpsertSourceChunkParams(
                        request,
                        Uid_,
                        NextChunkIndex_,
                        Chunks_[NextChunkIndex_]);
                });
            return;
        }

        // The modules row is published last: it is what makes the upload
        // visible, and nothing must find it before all of its chunks are in.
        Step_ = EStep::FlipModule;
        const bool withManifest = true;
        ExecuteYqlAsSystem(
            SelfId(),
            NQuery::BuildFlipModuleQuery(NUdfStore::GetModulesTablePath(), withManifest),
            false,
            [this](Ydb::Table::ExecuteDataQueryRequest& request) {
                NQuery::TModuleRow row;
                row.Name = Name_;
                row.Uid = Uid_;
                row.Md5 = Md5_;
                row.Size = Body_.size();
                row.Type = Type_;
                row.Version = Params_.version();
                row.ChunkCount = Chunks_.size();
                row.Manifest = TString(Params_.manifest_json());

                NQuery::TWriteConditions conditions;
                conditions.RequireAbsent = WriteMode() == Ydb::Udf::CREATE_ONLY;
                conditions.RequirePresent = WriteMode() == Ydb::Udf::REPLACE_ONLY;
                conditions.ExpectedUid = TString(Params_.expected_uid());

                NQuery::SetFlipModuleParams(
                    request,
                    row,
                    conditions,
                    Params_.version() != 0,
                    withManifest);
            });
    }

    //! Removes the chunks of `uid`, which by now nothing can reach: either the
    //! previous body of a module just replaced, or the body of an upload that
    //! never got published. Failing to collect them wastes space but does not
    //! make the answer to this call any less true, hence best effort.
    void StartCleanup(const TString& uid) {
        if (uid.empty()) {
            Finish();
            return;
        }
        Step_ = EStep::CleanupChunks;
        CleanupUid_ = uid;
        ExecuteYqlAsSystem(
            SelfId(),
            NQuery::BuildDeleteSourceChunksQuery(NUdfStore::GetModuleChunksTablePath()),
            false,
            [this](Ydb::Table::ExecuteDataQueryRequest& request) {
                NQuery::SetDeleteSourceChunksParams(request, CleanupUid_);
            });
    }

    void FailAndCleanup(Ydb::StatusIds::StatusCode status, const TString& error, const TString& uid) {
        PendingStatus_ = status;
        PendingError_ = error;
        StartCleanup(uid);
    }

    void Finish() {
        if (PendingStatus_ == Ydb::StatusIds::SUCCESS) {
            ReplySuccess();
        } else {
            ReplyError(PendingStatus_, PendingError_);
        }
    }

    void ReplySuccess() {
        Ydb::Udf::UploadModuleResult result;
        result.set_name(Name_);
        result.set_uid(Uid_);
        result.set_md5(Md5_);
        result.set_size(Body_.size());
        result.set_replaced_existing(ReplacedExisting_);

        YDB_LOG_INFO("UdfService: uploaded module",
            {"name", Name_},
            {"uid", Uid_},
            {"size", Body_.size()},
            {"chunks", Chunks_.size()});

        Send(ReplyTo_, new TEvUploadModuleResult(std::move(result)));
        PassAway();
    }

    void ReplyError(Ydb::StatusIds::StatusCode status, const TString& error) {
        YDB_LOG_WARN("UdfService: upload with error",
            {"name", Name_},
            {"error", error});
        Send(ReplyTo_, new TEvUploadModuleResult(status, error));
        PassAway();
    }

private:
    const TActorId ReplyTo_;
    const Ydb::Udf::UploadModuleParams Params_;
    const TString Body_;

    EStep Step_ = EStep::UpsertChunk;
    EUdfType Type_ = EUdfType::WASM;
    TString Name_;
    TString Md5_;
    TString Uid_;
    TString CleanupUid_;
    bool ReplacedExisting_ = false;
    TVector<TString> Chunks_;
    size_t NextChunkIndex_ = 0;

    //! The answer this call has already settled on, held back until the chunks
    //! nobody can reach have been collected.
    Ydb::StatusIds::StatusCode PendingStatus_ = Ydb::StatusIds::SUCCESS;
    TString PendingError_;
};

class TDeleteModuleActor: public TActorBootstrapped<TDeleteModuleActor> {
    enum class EStep {
        DeleteModule,
        ListArtifactDir,
        DeleteArtifacts,
    };

public:
    TDeleteModuleActor(
        const TActorId& replyTo,
        const Ydb::Udf::DeleteModuleRequest& request,
        const TString& databaseName)
        : ReplyTo_(replyTo)
        , Request_(request)
        , DatabaseName_(databaseName)
    {
    }

    void Bootstrap() {
        Become(&TDeleteModuleActor::StateMain);

        TString kindError;
        const auto kindStatus = ValidateKind(Request_.module_kind(), kindError);
        if (kindStatus != Ydb::StatusIds::SUCCESS) {
            ReplyError(kindStatus, kindError);
            return;
        }

        if (!IsWasmUdfEnabled()) {
            ReplyError(Ydb::StatusIds::PRECONDITION_FAILED,
                "UDF store with WASM support is not enabled on this cluster");
            return;
        }

        Name_ = Strip(TString(Request_.name()));
        if (Name_.empty()) {
            ReplyError(Ydb::StatusIds::BAD_REQUEST, "name is required");
            return;
        }
        if (Request_.module_type() != Ydb::Udf::MODULE_TYPE_UNSPECIFIED) {
            EUdfType requestedType = EUdfType::WASM;
            if (!FromProtoType(Request_.module_type(), requestedType)) {
                ReplyError(Ydb::StatusIds::BAD_REQUEST, "module_type must be module or library");
                return;
            }
            RequiredType_ = requestedType;
        }

        // Row and chunks go in one transaction: deleting them one after the
        // other leaves a window in which the module is still listed with a body
        // already gone, which is worse than either end state.
        Step_ = EStep::DeleteModule;
        ExecuteYqlAsSystem(
            SelfId(),
            NQuery::BuildDeleteModuleQuery(
                NUdfStore::GetModulesTablePath(),
                NUdfStore::GetModuleChunksTablePath()),
            false,
            [this](Ydb::Table::ExecuteDataQueryRequest& request) {
                NQuery::SetDeleteModuleParams(
                    request,
                    Name_,
                    TString(Request_.expected_uid()),
                    RequiredType_);
            });
    }

    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvYqlResult, Handle);
            hFunc(NMetadata::NRequest::TEvRequestFailed, Handle);
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            default:
                break;
        }
    }

private:
    void Handle(TEvYqlResult::TPtr& ev) {
        switch (Step_) {
            case EStep::DeleteModule: {
                NQuery::TModulePreState preState;
                if (!NQuery::ParseModulePreStateResponse(ev->Get()->GetResult(), preState)) {
                    ReplyError(Ydb::StatusIds::INTERNAL_ERROR,
                        "UDF store did not report the state of the module row");
                    return;
                }
                if (!preState.Applied) {
                    Ydb::StatusIds::StatusCode status = Ydb::StatusIds::INTERNAL_ERROR;
                    TString error;
                    ExplainRejectedDelete(preState, status, error);
                    ReplyError(status, error);
                    return;
                }

                Uid_ = preState.Uid;
                ArtifactKind_ = NQuery::ArtifactKindFor(preState.Type);
                // The module is gone as far as anyone querying it is
                // concerned; the artifacts are just disk space now.
                Step_ = EStep::ListArtifactDir;
                Send(MakeSchemeCacheID(), MakeArtifactDirListingRequest(DatabaseName_));
                return;
            }
            case EStep::DeleteArtifacts:
                ++NextArtifactQueryIndex_;
                DeleteNextArtifacts();
                return;
            case EStep::ListArtifactDir:
                return;
        }
    }

    //! Names the condition the delete transaction found broken, read off the
    //! pre-state it evaluated the condition on.
    void ExplainRejectedDelete(
        const NQuery::TModulePreState& preState,
        Ydb::StatusIds::StatusCode& status,
        TString& error) const {
        if (!preState.Existed) {
            status = Ydb::StatusIds::NOT_FOUND;
            error = TStringBuilder() << "module '" << Name_ << "' does not exist";
            return;
        }
        if (!Request_.expected_uid().empty() && preState.Uid != Request_.expected_uid()) {
            status = Ydb::StatusIds::ABORTED;
            error = TStringBuilder()
                << "module '" << Name_ << "' has uid " << preState.Uid
                << ", which is not the expected " << Request_.expected_uid();
            return;
        }
        if (!preState.TypeKnown || preState.Type == EUdfType::NATIVE_UNSAFE) {
            status = Ydb::StatusIds::PRECONDITION_FAILED;
            error = TStringBuilder()
                << "module '" << Name_ << "' is " << preState.TypeName
                << ", which this API does not manage";
            return;
        }
        if (RequiredType_ && preState.Type != *RequiredType_) {
            status = Ydb::StatusIds::PRECONDITION_FAILED;
            error = TStringBuilder()
                << "module '" << Name_ << "' is " << preState.TypeName
                << ", which does not match the requested kind";
            return;
        }
        status = Ydb::StatusIds::INTERNAL_ERROR;
        error = TStringBuilder() << "delete of '" << Name_ << "' was refused by the UDF store for no known reason";
    }

    void Handle(NMetadata::NRequest::TEvRequestFailed::TPtr& ev) {
        if (Step_ == EStep::DeleteArtifacts) {
            YDB_LOG_WARN("UdfService: best-effort artifact cleanup",
                {"name", Name_},
                {"tblePath", ArtifactQueries_[NextArtifactQueryIndex_].TablePath},
                {"errorMessage", ev->Get()->GetErrorMessage()});
            ++NextArtifactQueryIndex_;
            DeleteNextArtifacts();
            return;
        }
        ReplyError(ev->Get()->GetStatus(), TStringBuilder()
                                               << "UDF store delete failed at step " << static_cast<int>(Step_) << ": " << ev->Get()->GetErrorMessage());
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        if (Step_ != EStep::ListArtifactDir) {
            return;
        }
        TVector<TString> cpuSpecs;
        if (!ParseArtifactDirListing(*ev->Get()->Request, cpuSpecs)) {
            YDB_LOG_WARN("UdfService: artifacts directory of not listed, leaving artifacts behind",
                {"name", Name_});
            ReplySuccess();
            return;
        }
        for (const auto& cpuSpec : cpuSpecs) {
            // Chunks first: the reverse order would leave chunks nobody can
            // find if this actor dies in between.
            ArtifactQueries_.push_back({NUdfStore::GetArtifactChunksTablePath(cpuSpec)});
            ArtifactQueries_.push_back({NUdfStore::GetArtifactTablePath(cpuSpec)});
        }
        DeleteNextArtifacts();
    }

    void DeleteNextArtifacts() {
        if (NextArtifactQueryIndex_ >= ArtifactQueries_.size()) {
            ReplySuccess();
            return;
        }
        Step_ = EStep::DeleteArtifacts;
        const TString& tablePath = ArtifactQueries_[NextArtifactQueryIndex_].TablePath;
        ExecuteYqlAsSystem(
            SelfId(),
            NQuery::BuildDeleteArtifactsByIdQuery(tablePath),
            false,
            [this](Ydb::Table::ExecuteDataQueryRequest& request) {
                NQuery::SetDeleteArtifactsByIdParams(request, Name_, ArtifactKind_);
            });
    }

    void ReplySuccess() {
        YDB_LOG_INFO("UdfService: deleted module",
            {"name", Name_},
            {"uid", Uid_});
        Send(ReplyTo_, new TEvDeleteModuleResult(Ydb::Udf::DeleteModuleResult()));
        PassAway();
    }

    void ReplyError(Ydb::StatusIds::StatusCode status, const TString& error) {
        YDB_LOG_WARN("UdfService: delete of",
            {"name", Name_},
            {"failed", error});
        Send(ReplyTo_, new TEvDeleteModuleResult(status, error));
        PassAway();
    }

private:
    struct TArtifactQuery {
        TString TablePath;
    };

    const TActorId ReplyTo_;
    const Ydb::Udf::DeleteModuleRequest Request_;
    const TString DatabaseName_;

    EStep Step_ = EStep::DeleteModule;
    TString Name_;
    TString Uid_;
    TMaybe<EUdfType> RequiredType_;
    TString ArtifactKind_;
    TVector<TArtifactQuery> ArtifactQueries_;
    size_t NextArtifactQueryIndex_ = 0;
};

} // namespace

IActor* CreateUploadModuleActor(
    const TActorId& replyTo,
    const Ydb::Udf::UploadModuleParams& params,
    TString body)
{
    return new TUploadModuleActor(replyTo, params, std::move(body));
}

IActor* CreateDeleteModuleActor(
    const TActorId& replyTo,
    const Ydb::Udf::DeleteModuleRequest& request,
    const TString& databaseName)
{
    return new TDeleteModuleActor(replyTo, request, databaseName);
}

} // namespace NKikimr::NUdfApi
