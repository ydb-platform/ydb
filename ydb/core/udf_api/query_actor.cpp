#include "query_actor.h"

#include "common.h"
#include "events.h"
#include "table_query.h"

#include <ydb/services/udf_store/metadata_subscription/storage_paths.h>
#include <ydb/services/metadata/request/common.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/strip.h>

namespace NKikimr::NUdfApi {

using namespace NActors;

using NUdfStore::ECompileStatus;
using NUdfStore::EUdfType;

namespace {

using TEvYqlResult = NMetadata::NRequest::TEvRequestResult<NMetadata::NRequest::TDialogYQLRequest>;

constexpr ui64 DefaultPageSize = 100;
constexpr ui64 MaxPageSize = 1000;

class TListModulesActor: public TActorBootstrapped<TListModulesActor> {
public:
    TListModulesActor(const TActorId& replyTo, const Ydb::Udf::ListModulesRequest& request)
        : ReplyTo_(replyTo)
        , Request_(request)
    {}

    void Bootstrap() {
        Become(&TListModulesActor::StateMain);

        if (!IsWasmUdfEnabled()) {
            ReplyError(Ydb::StatusIds::PRECONDITION_FAILED,
                "UDF store with WASM support is not enabled on this cluster");
            return;
        }

        if (!Request_.page_token().empty() && !TryFromString(Request_.page_token(), Offset_)) {
            ReplyError(Ydb::StatusIds::BAD_REQUEST,
                TStringBuilder() << "page_token '" << Request_.page_token() << "' is not a valid page token");
            return;
        }
        Limit_ = Request_.page_size() ? Min<ui64>(Request_.page_size(), MaxPageSize) : DefaultPageSize;

        if (Request_.kind_filter() != Ydb::Udf::MODULE_KIND_UNSPECIFIED) {
            EUdfType type = EUdfType::WASM;
            if (!FromProtoKind(Request_.kind_filter(), type)) {
                ReplyError(Ydb::StatusIds::BAD_REQUEST, "kind_filter must be UDF or LIBRARY");
                return;
            }
            Filter_.Type = type;
        }
        if (Request_.status_filter() != Ydb::Udf::COMPILE_STATUS_UNSPECIFIED) {
            ECompileStatus status = ECompileStatus::Pending;
            if (!FromProtoCompileStatus(Request_.status_filter(), status)) {
                ReplyError(Ydb::StatusIds::BAD_REQUEST, "status_filter is not a known compile status");
                return;
            }
            Filter_.CompileStatus = status;
        }

        ExecuteYqlAsSystem(
            SelfId(),
            NQuery::BuildListModulesQuery(NUdfStore::GetModulesTablePath(), Filter_),
            true,
            [this](Ydb::Table::ExecuteDataQueryRequest& request) {
                NQuery::SetListModulesParams(request, Filter_, Offset_, Limit_);
            });
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
    void Handle(TEvYqlResult::TPtr& ev) {
        TVector<NQuery::TModuleRow> rows;
        if (!NQuery::ParseModuleRowsResponse(ev->Get()->GetResult(), rows)) {
            ReplyError(Ydb::StatusIds::INTERNAL_ERROR, "failed to read the modules table");
            return;
        }

        Ydb::Udf::ListModulesResult result;
        for (const auto& row : rows) {
            FillModuleInfo(row, *result.add_modules());
        }
        // A full page is the only hint that more rows may follow: the query
        // does not count what it did not read.
        if (rows.size() == Limit_) {
            result.set_next_page_token(ToString(Offset_ + Limit_));
        }

        Send(ReplyTo_, new TEvListModulesResult(std::move(result)));
        PassAway();
    }

    void Handle(NMetadata::NRequest::TEvRequestFailed::TPtr& ev) {
        ReplyError(Ydb::StatusIds::INTERNAL_ERROR,
            TStringBuilder() << "UDF store read failed: " << ev->Get()->GetErrorMessage());
    }

    void ReplyError(Ydb::StatusIds::StatusCode status, const TString& error) {
        Send(ReplyTo_, new TEvListModulesResult(status, error));
        PassAway();
    }

private:
    const TActorId ReplyTo_;
    const Ydb::Udf::ListModulesRequest Request_;

    NQuery::TListFilter Filter_;
    ui64 Offset_ = 0;
    ui64 Limit_ = DefaultPageSize;
};

class TDescribeModuleActor: public TActorBootstrapped<TDescribeModuleActor> {
    enum class EStep {
        SelectModule,
        ListArtifactDir,
        SelectArtifact,
    };

public:
    TDescribeModuleActor(
            const TActorId& replyTo,
            const Ydb::Udf::DescribeModuleRequest& request,
            const TString& databaseName)
        : ReplyTo_(replyTo)
        , Request_(request)
        , DatabaseName_(databaseName)
    {}

    void Bootstrap() {
        Become(&TDescribeModuleActor::StateMain);

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

        Step_ = EStep::SelectModule;
        ExecuteYqlAsSystem(
            SelfId(),
            NQuery::BuildSelectModuleByNameQuery(NUdfStore::GetModulesTablePath()),
            true,
            [this](Ydb::Table::ExecuteDataQueryRequest& request) {
                NQuery::SetSelectModuleByNameParams(request, Name_);
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
            case EStep::SelectModule: {
                NQuery::TModuleRow row;
                if (!NQuery::ParseModuleRowResponse(ev->Get()->GetResult(), row)) {
                    ReplyError(Ydb::StatusIds::NOT_FOUND,
                        TStringBuilder() << "module '" << Name_ << "' does not exist");
                    return;
                }
                FillModuleInfo(row, *Result_.mutable_module());
                Result_.set_manifest_json(row.Manifest);
                Uid_ = row.Uid;
                ArtifactKind_ = NQuery::ArtifactKindFor(row.Type);

                Step_ = EStep::ListArtifactDir;
                Send(MakeSchemeCacheID(), MakeArtifactDirListingRequest(DatabaseName_));
                return;
            }
            case EStep::SelectArtifact: {
                bool ready = false;
                if (!NQuery::ParseArtifactReadyResponse(ev->Get()->GetResult(), ready)) {
                    ReplyError(Ydb::StatusIds::INTERNAL_ERROR, TStringBuilder()
                        << "failed to read the artifact table of cpu_spec '" << CpuSpecs_[NextCpuSpecIndex_] << "'");
                    return;
                }
                auto& platform = *Result_.add_platforms();
                platform.set_cpu_spec(CpuSpecs_[NextCpuSpecIndex_]);
                platform.set_status(ready ? Ydb::Udf::READY : Ydb::Udf::PENDING);
                ++NextCpuSpecIndex_;
                SelectNextArtifact();
                return;
            }
            case EStep::ListArtifactDir:
                return;
        }
    }

    void Handle(NMetadata::NRequest::TEvRequestFailed::TPtr& ev) {
        ReplyError(Ydb::StatusIds::INTERNAL_ERROR, TStringBuilder()
            << "UDF store read failed at step " << static_cast<int>(Step_) << ": " << ev->Get()->GetErrorMessage());
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        if (Step_ != EStep::ListArtifactDir) {
            return;
        }
        if (!ParseArtifactDirListing(*ev->Get()->Request, CpuSpecs_)) {
            ALS_WARN(NKikimrServices::GRPC_SERVER)
                << "UdfService: artifacts directory not listed while describing '" << Name_ << "'";
            // Reporting no platform at all is honest: the request cannot tell
            // whether any exists, and claiming PENDING would be a guess.
            ReplySuccess();
            return;
        }
        SelectNextArtifact();
    }

    void SelectNextArtifact() {
        if (NextCpuSpecIndex_ >= CpuSpecs_.size()) {
            ReplySuccess();
            return;
        }
        Step_ = EStep::SelectArtifact;
        ExecuteYqlAsSystem(
            SelfId(),
            NQuery::BuildSelectArtifactReadyQuery(NUdfStore::GetArtifactTablePath(CpuSpecs_[NextCpuSpecIndex_])),
            true,
            [this](Ydb::Table::ExecuteDataQueryRequest& request) {
                NQuery::SetSelectArtifactReadyParams(request, Name_, ArtifactKind_, Uid_);
            });
    }

    void ReplySuccess() {
        Send(ReplyTo_, new TEvDescribeModuleResult(std::move(Result_)));
        PassAway();
    }

    void ReplyError(Ydb::StatusIds::StatusCode status, const TString& error) {
        Send(ReplyTo_, new TEvDescribeModuleResult(status, error));
        PassAway();
    }

private:
    const TActorId ReplyTo_;
    const Ydb::Udf::DescribeModuleRequest Request_;
    const TString DatabaseName_;

    EStep Step_ = EStep::SelectModule;
    TString Name_;
    TString Uid_;
    TString ArtifactKind_;
    TVector<TString> CpuSpecs_;
    size_t NextCpuSpecIndex_ = 0;
    Ydb::Udf::DescribeModuleResult Result_;
};

} // namespace

IActor* CreateListModulesActor(const TActorId& replyTo, const Ydb::Udf::ListModulesRequest& request) {
    return new TListModulesActor(replyTo, request);
}

IActor* CreateDescribeModuleActor(
    const TActorId& replyTo,
    const Ydb::Udf::DescribeModuleRequest& request,
    const TString& databaseName)
{
    return new TDescribeModuleActor(replyTo, request, databaseName);
}

} // namespace NKikimr::NUdfApi
