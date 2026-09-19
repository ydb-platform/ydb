#include "query_actor.h"
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/path.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/services/udf_store/compile_controller/events.h>
#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>

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
    {
    }

    void Bootstrap() {
        Become(&TListModulesActor::StateMain);

        TString kindError;
        const auto kindStatus = ValidateKind(Request_.kind_filter(), kindError);
        if (kindStatus != Ydb::StatusIds::SUCCESS) {
            ReplyError(kindStatus, kindError);
            return;
        }

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

        if (Request_.type_filter() != Ydb::Udf::MODULE_TYPE_UNSPECIFIED) {
            EUdfType type = EUdfType::WASM;
            if (!FromProtoType(Request_.type_filter(), type)) {
                ReplyError(Ydb::StatusIds::BAD_REQUEST, "type_filter must be module or library");
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
        ResolveController,
        ReadController,
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
    {
    }

    void Bootstrap() {
        Become(&TDescribeModuleActor::StateMain);
        Schedule(TDuration::Seconds(30), new TEvents::TEvWakeup());

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
            hFunc(NUdfStore::TEvCompileController::TEvDescribeModuleResult, Handle);
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            cFunc(TEvents::TEvWakeup::EventType, HandleTimeout);
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
                if (row.Type == EUdfType::NATIVE_UNSAFE) {
                    ReplyError(Ydb::StatusIds::PRECONDITION_FAILED, TString(NativeUnsupported));
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
                FillPlatform(platform, CpuSpecs_[NextCpuSpecIndex_], ready);
                ++NextCpuSpecIndex_;
                SelectNextArtifact();
                return;
            }
            case EStep::ListArtifactDir:
            case EStep::ResolveController:
            case EStep::ReadController:
                return;
        }
    }

    void Handle(NMetadata::NRequest::TEvRequestFailed::TPtr& ev) {
        ReplyError(Ydb::StatusIds::INTERNAL_ERROR, TStringBuilder()
            << "UDF store read failed at step " << static_cast<int>(Step_) << ": " << ev->Get()->GetErrorMessage());
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        if (Step_ == EStep::ListArtifactDir) {
            if (!ParseArtifactDirListing(*ev->Get()->Request, CpuSpecs_)) {
                ReplyError(Ydb::StatusIds::UNAVAILABLE, "Cannot enumerate UDF artifact platforms");
                return;
            }
            for (const auto& cpuSpec : CpuSpecs_) {
                ArtifactTables_.insert(cpuSpec);
            }
            Step_ = EStep::ResolveController;
            Send(MakeSchemeCacheID(), MakeDatabaseOwnerRequest(
                                          DatabaseName_.empty() ? AppData()->TenantName : DatabaseName_));
            return;
        }
        if (Step_ != EStep::ResolveController) {
            return;
        }
        const auto& response = *ev->Get()->Request;
        if (response.ResultSet.size() != 1 || response.ErrorCount ||
            response.ResultSet.front().Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok ||
            !response.ResultSet.front().DomainInfo) {
            ReplyError(Ydb::StatusIds::UNAVAILABLE, "Cannot resolve UDF compile controller");
            return;
        }
        const auto& domain = response.ResultSet.front().DomainInfo;
        if (domain->IsServerless() && ev->Cookie != 1) {
            auto navigate = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
            navigate->DatabaseName = AppData()->DomainsInfo->GetDomain()->Name;
            auto& entry = navigate->ResultSet.emplace_back();
            entry.TableId = TTableId(domain->ResourcesDomainKey.OwnerId, domain->ResourcesDomainKey.LocalPathId);
            entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
            entry.RequestType = NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByTableId;
            entry.RedirectRequired = false;
            entry.ShowPrivatePath = true;
            Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(navigate.Release()), 0, 1);
            return;
        }
        const ui64 tablet = domain->Params.GetWasmCompileController();
        if (!tablet) {
            SelectNextArtifact();
            return;
        }
        Step_ = EStep::ReadController;
        NTabletPipe::TClientConfig config;
        config.RetryPolicy.RetryLimitCount = 3;
        ControllerPipe_ = Register(NTabletPipe::CreateClient(SelfId(), tablet, config));
        auto request = MakeHolder<NUdfStore::TEvCompileController::TEvDescribeModule>();
        request->Record.SetName(Name_);
        request->Record.SetUid(Uid_);
        request->Record.SetKind(ArtifactKind_ == "library"
                                    ? NKikimrUdfStore::ARTIFACT_KIND_LIBRARY
                                    : NKikimrUdfStore::ARTIFACT_KIND_MODULE);
        NTabletPipe::SendData(SelfId(), ControllerPipe_, request.Release());
    }

    void Handle(NUdfStore::TEvCompileController::TEvDescribeModuleResult::TPtr& ev) {
        if (Step_ != EStep::ReadController) {
            return;
        }
        for (const auto& platform : ev->Get()->Record.GetPlatforms()) {
            ControllerPlatforms_[platform.GetCpuSpec()] = platform;
            CpuSpecs_.push_back(platform.GetCpuSpec());
        }
        SortUnique(CpuSpecs_);
        SelectNextArtifact();
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        if (Step_ == EStep::ReadController && ev->Get()->Status != NKikimrProto::OK) {
            ReplyError(Ydb::StatusIds::UNAVAILABLE, "UDF compile controller is unavailable");
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr&) {
        if (Step_ == EStep::ReadController) {
            ReplyError(Ydb::StatusIds::UNAVAILABLE, "UDF compile controller disconnected");
        }
    }

    void HandleTimeout() {
        ReplyError(Ydb::StatusIds::TIMEOUT, "Timed out reading UDF platform status");
    }

    void PassAway() override {
        if (ControllerPipe_) {
            NTabletPipe::CloseClient(SelfId(), ControllerPipe_);
        }
        TActorBootstrapped::PassAway();
    }

    void FillPlatform(Ydb::Udf::PlatformCompileStatus& result, const TString& cpuSpec, bool ready) {
        result.set_cpu_spec(cpuSpec);
        result.set_status(ready ? Ydb::Udf::READY : Ydb::Udf::PENDING);
        if (!ready) {
            if (const auto it = ControllerPlatforms_.find(cpuSpec); it != ControllerPlatforms_.end()) {
                if (it->second.GetFailed()) {
                    result.set_status(Ydb::Udf::FAILED);
                    result.set_compile_error(it->second.GetError());
                } else if (it->second.GetCompiling()) {
                    result.set_status(Ydb::Udf::COMPILING);
                }
            }
        }
    }

    void SelectNextArtifact() {
        while (NextCpuSpecIndex_ < CpuSpecs_.size() && !ArtifactTables_.contains(CpuSpecs_[NextCpuSpecIndex_])) {
            FillPlatform(*Result_.add_platforms(), CpuSpecs_[NextCpuSpecIndex_++], false);
        }
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
    THashSet<TString> ArtifactTables_;
    THashMap<TString, NKikimrUdfStore::TEvDescribeModuleResult::TPlatform> ControllerPlatforms_;
    TActorId ControllerPipe_;
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
