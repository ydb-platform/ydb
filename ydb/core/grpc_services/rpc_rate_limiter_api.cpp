#include "service_ratelimiter.h"
#include "service_ratelimiter_events.h"

#include "rpc_calls.h"
#include "rpc_scheme_base.h"
#include "rpc_common/rpc_common.h"

#include <ydb/core/quoter/public/quoter.h>
#include <ydb/core/kesus/tablet/events.h>

namespace NKikimr::NGRpcService {

using namespace NActors;
using namespace Ydb;
using namespace NKesus;

namespace {

template <typename TDerived, typename TRequest>
class TRateLimiterRequest : public TRpcOperationRequestActor<TDerived, TRequest> {
public:
    using TBase = TRpcOperationRequestActor<TDerived, TRequest>;

    TRateLimiterRequest(IRequestOpCtx* msg, bool trusted = false)
        : TBase(msg)
        , TrustedZone(trusted)
    {}

    static bool ValidateMetric (const Ydb::RateLimiter::MeteringConfig::Metric& srcMetric, Ydb::StatusIds::StatusCode& status, NYql::TIssues& issues) {
        static const TSet<TString> supportedFields{
            {"version"},
            {"schema"},
            {"cloud_id"},
            {"folder_id"},
            {"resource_id"},
            {"source_id"},
        };
        auto& metricFields = srcMetric.metric_fields().fields();

        for (auto& [key, field] : metricFields) {
            if (supportedFields.count(key) == 0) {
                status = StatusIds::BAD_REQUEST;
                issues.AddIssue(TStringBuilder() << "Unsupported key for metric. Key: " << key << ".");
                return false;
            }
            if (!field.has_string_value()) {
                status = StatusIds::BAD_REQUEST;
                issues.AddIssue(TStringBuilder() << "Unsupported type for metric. Key: " << key << ".");
                return false;
            }
        }

        return true;
    };

    bool ValidateResource(const Ydb::RateLimiter::Resource& resource, Ydb::StatusIds::StatusCode& status, NYql::TIssues& issues) {
        if (!ValidateResourcePath(resource.resource_path(), status, issues)) {
            return false;
        }

        if (resource.type_case() == Ydb::RateLimiter::Resource::TYPE_NOT_SET) {
            status = StatusIds::BAD_REQUEST;
            issues.AddIssue("No resource properties.");
            return false;
        }

        if (resource.has_metering_config()) {
            auto self = static_cast<TDerived*>(this);
            const auto& userTokenStr = self->Request_->GetSerializedToken();
            bool allowed = AppData()->AdministrationAllowedSIDs.empty();
            if (userTokenStr) {
                NACLib::TUserToken userToken(userTokenStr);
                for (auto &sid : AppData()->AdministrationAllowedSIDs) {
                    if (userToken.IsExist(sid)) {
                        allowed = true;
                        break;
                    }
                }
            }

            if (!allowed) {
                status = StatusIds::UNAUTHORIZED;
                issues.AddIssue("Setting metering is allowed only for administrators");
                return false;
            }

            const auto& acc = resource.metering_config();

            if (acc.has_provisioned() && !ValidateMetric(acc.provisioned(), status, issues)) {
                return false;
            }
            if (acc.has_on_demand() && !ValidateMetric(acc.on_demand(), status, issues)) {
                return false;
            }
            if (acc.has_overshoot() && !ValidateMetric(acc.overshoot(), status, issues)) {
                return false;
            }
        }

        return true;
    }

    bool ValidateResourcePath(const TString& path, Ydb::StatusIds::StatusCode& status, NYql::TIssues& issues) {
        if (path != CanonizeQuoterResourcePath(path)) {
            status = StatusIds::BAD_REQUEST;
            issues.AddIssue("Bad resource path.");
            return false;
        }
        return true;
    }

    bool ValidateCoordinationNodePath(Ydb::StatusIds::StatusCode& status, NYql::TIssues& issues) {
        auto databaseName = this->Request_->GetDatabaseName()
            .GetOrElse(DatabaseFromDomain(AppData()));

        if (!TrustedZone && !GetCoordinationNodePath().StartsWith(databaseName)) {
            status = StatusIds::BAD_REQUEST;
            issues.AddIssue(TStringBuilder()
                << "Coordination node path: " << GetCoordinationNodePath()
                << " does not belong to current database: " << databaseName
                << ".");
            return false;
        }
        return true;
    }

protected:
    const TString& GetCoordinationNodePath() const {
        return this->GetProtoRequest()->coordination_node_path();
    }

private:
    const bool TrustedZone;
};

template <class TEvRequest>
class TRateLimiterControlRequest : public TRateLimiterRequest<TRateLimiterControlRequest<TEvRequest>, TEvRequest> {
public:
    using TBase = TRateLimiterRequest<TRateLimiterControlRequest<TEvRequest>, TEvRequest>;
    using TBase::TBase;

    void Bootstrap(const TActorContext& ctx) {
        TBase::Bootstrap(ctx);

        this->UnsafeBecome(&TRateLimiterControlRequest::StateFunc);

        Ydb::StatusIds::StatusCode status = Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
        NYql::TIssues issues;

        if (!this->ValidateCoordinationNodePath(status, issues)) {
            this->Reply(status, issues, this->ActorContext());
            return;
        }

        if (!ValidateRequest(status, issues)) {
            this->Reply(status, issues, this->ActorContext());
            return;
        }

        ResolveCoordinationPath();
    }

protected:
    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
        default:
            return TBase::StateFuncBase(ev);
        }
    }

    void ResolveCoordinationPath() {
        TVector<TString> path = NKikimr::SplitPath(this->GetCoordinationNodePath());
        if (path.empty()) {
            this->Reply(StatusIds::BAD_REQUEST, "Empty path.", NKikimrIssues::TIssuesIds::GENERIC_RESOLVE_ERROR, this->ActorContext());
            return;
        }

        auto req = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        req->ResultSet.emplace_back();
        req->ResultSet.back().Path.swap(path);
        req->ResultSet.back().Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
        this->Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(req), 0, 0);
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        THolder<NSchemeCache::TSchemeCacheNavigate> navigate = std::move(ev->Get()->Request);
        if (navigate->ResultSet.size() != 1 || navigate->ErrorCount > 0) {
            this->Reply(StatusIds::INTERNAL_ERROR, this->ActorContext());
            return;
        }

        const auto& entry = navigate->ResultSet.front();
        if (entry.Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
            this->Reply(StatusIds::SCHEME_ERROR, this->ActorContext());
            return;
        }

        if (entry.Kind != NSchemeCache::TSchemeCacheNavigate::KindKesus) {
            this->Reply(StatusIds::BAD_REQUEST, "Path is not a coordination node path.", NKikimrIssues::TIssuesIds::GENERIC_RESOLVE_ERROR, this->ActorContext());
            return;
        }

        if (!entry.KesusInfo) {
            this->Reply(StatusIds::BAD_REQUEST, "Bad request: no coordination node info found.", NKikimrIssues::TIssuesIds::GENERIC_RESOLVE_ERROR, this->ActorContext());
            return;
        }

        KesusTabletId = entry.KesusInfo->Description.GetKesusTabletId();

        if (!KesusTabletId) {
            this->Reply(StatusIds::BAD_REQUEST, "Bad request: no coordination node id found.", NKikimrIssues::TIssuesIds::GENERIC_RESOLVE_ERROR, this->ActorContext());
            return;
        }

        CreatePipe();

        SendRequest();
    }

    NTabletPipe::TClientConfig GetPipeConfig() {
        NTabletPipe::TClientConfig cfg;
        cfg.RetryPolicy = {
            .RetryLimitCount = 3u
        };
        return cfg;
    }

    void CreatePipe() {
        KesusPipeClient = this->Register(NTabletPipe::CreateClient(this->SelfId(), KesusTabletId, GetPipeConfig()));
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        if (ev->Get()->Status != NKikimrProto::OK) {
            this->Reply(StatusIds::UNAVAILABLE, "Failed to connect to coordination node.", NKikimrIssues::TIssuesIds::SHARD_NOT_AVAILABLE, this->ActorContext());
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr&) {
        this->Reply(StatusIds::UNAVAILABLE, "Connection to coordination node was lost.", NKikimrIssues::TIssuesIds::SHARD_NOT_AVAILABLE, this->ActorContext());
    }

    void ReplyFromKesusError(const NKikimrKesus::TKesusError& err) {
        this->Reply(err.GetStatus(), err.GetIssues(), this->ActorContext());
    }

    virtual bool ValidateRequest(Ydb::StatusIds::StatusCode& status, NYql::TIssues& issues) = 0;

    virtual void SendRequest() = 0;

    void PassAway() override {
        if (KesusPipeClient) {
            NTabletPipe::CloseClient(this->SelfId(), KesusPipeClient);
            KesusPipeClient = {};
        }
        TBase::PassAway();
    }

protected:
    ui64 KesusTabletId = 0;
    TActorId KesusPipeClient;
};

static void CopyProps(const Ydb::RateLimiter::Resource& src, NKikimrKesus::TStreamingQuoterResource& dst) {
    dst.SetResourcePath(src.resource_path());
    const auto& srcProps = src.hierarchical_drr();
    auto& props = *dst.MutableHierarchicalDRRResourceConfig();
    props.SetMaxUnitsPerSecond(srcProps.max_units_per_second());
    props.SetMaxBurstSizeCoefficient(srcProps.max_burst_size_coefficient());
    props.SetPrefetchCoefficient(srcProps.prefetch_coefficient());
    props.SetPrefetchWatermark(srcProps.prefetch_watermark());
    if (srcProps.has_immediately_fill_up_to()) {
        props.SetImmediatelyFillUpTo(srcProps.immediately_fill_up_to());
    }
    if (src.has_metering_config()) {
        const auto& srcAcc = src.metering_config();
        auto& acc = *dst.MutableAccountingConfig();
        acc.SetEnabled(srcAcc.enabled());
        acc.SetReportPeriodMs(srcAcc.report_period_ms());
        acc.SetAccountPeriodMs(srcAcc.meter_period_ms());
        acc.SetCollectPeriodSec(srcAcc.collect_period_sec());
        acc.SetProvisionedUnitsPerSecond(srcAcc.provisioned_units_per_second());
        acc.SetProvisionedCoefficient(srcAcc.provisioned_coefficient());
        acc.SetOvershootCoefficient(srcAcc.overshoot_coefficient());
        auto copyMetric = [] (const Ydb::RateLimiter::MeteringConfig::Metric& srcMetric, NKikimrKesus::TAccountingConfig::TMetric& metric) {
            metric.SetEnabled(srcMetric.enabled());
            metric.SetBillingPeriodSec(srcMetric.billing_period_sec());
            *metric.MutableLabels() = srcMetric.labels();

            /* overwrite if we have new fields */
            /* TODO: support arbitrary fields in metering core */
            if (srcMetric.has_metric_fields()) {
                auto& metricFields = srcMetric.metric_fields().fields();
                if (metricFields.contains("version") && metricFields.at("version").has_string_value()) {
                    metric.SetVersion(metricFields.at("version").string_value());
                }
                if (metricFields.contains("schema") && metricFields.at("schema").has_string_value()) {
                    metric.SetSchema(metricFields.at("schema").string_value());
                }
                if (metricFields.contains("cloud_id") && metricFields.at("cloud_id").has_string_value()) {
                    metric.SetCloudId(metricFields.at("cloud_id").string_value());
                }
                if (metricFields.contains("folder_id") && metricFields.at("folder_id").has_string_value()) {
                    metric.SetFolderId(metricFields.at("folder_id").string_value());
                }
                if (metricFields.contains("resource_id") && metricFields.at("resource_id").has_string_value()) {
                    metric.SetResourceId(metricFields.at("resource_id").string_value());
                }
                if (metricFields.contains("source_id") && metricFields.at("source_id").has_string_value()) {
                    metric.SetSourceId(metricFields.at("source_id").string_value());
                }
            }
        };
        if (srcAcc.has_provisioned()) {
            copyMetric(srcAcc.provisioned(), *acc.MutableProvisioned());
        }
        if (srcAcc.has_on_demand()) {
            copyMetric(srcAcc.on_demand(), *acc.MutableOnDemand());
        }
        if (srcAcc.has_overshoot()) {
            copyMetric(srcAcc.overshoot(), *acc.MutableOvershoot());
        }
    }
    if (srcProps.has_replicated_bucket()) {
        const auto& srcRepl = srcProps.replicated_bucket();
        auto& repl = *props.MutableReplicatedBucket();
        if (srcRepl.has_report_interval_ms()) {
            repl.SetReportIntervalMs(srcRepl.report_interval_ms());
        }
    }
}

static void CopyProps(const NKikimrKesus::TStreamingQuoterResource& src, Ydb::RateLimiter::Resource& dst) {
    dst.set_resource_path(src.GetResourcePath());
    const auto& srcProps = src.GetHierarchicalDRRResourceConfig();
    auto& props = *dst.mutable_hierarchical_drr();
    props.set_max_units_per_second(srcProps.GetMaxUnitsPerSecond());
    props.set_max_burst_size_coefficient(srcProps.GetMaxBurstSizeCoefficient());
    props.set_prefetch_coefficient(srcProps.GetPrefetchCoefficient());
    props.set_prefetch_watermark(srcProps.GetPrefetchWatermark());
    if (srcProps.HasImmediatelyFillUpTo()) {
        props.set_immediately_fill_up_to(srcProps.GetImmediatelyFillUpTo());
    }
    if (src.HasAccountingConfig()) {
        const auto& srcAcc = src.GetAccountingConfig();
        auto& acc = *dst.mutable_metering_config();
        acc.set_enabled(srcAcc.GetEnabled());
        acc.set_report_period_ms(srcAcc.GetReportPeriodMs());
        acc.set_meter_period_ms(srcAcc.GetAccountPeriodMs());
        acc.set_collect_period_sec(srcAcc.GetCollectPeriodSec());
        acc.set_provisioned_units_per_second(srcAcc.GetProvisionedUnitsPerSecond());
        acc.set_provisioned_coefficient(srcAcc.GetProvisionedCoefficient());
        acc.set_overshoot_coefficient(srcAcc.GetOvershootCoefficient());
        auto copyMetric = [] (const NKikimrKesus::TAccountingConfig::TMetric& srcMetric, Ydb::RateLimiter::MeteringConfig::Metric& metric) {
            metric.set_enabled(srcMetric.GetEnabled());
            metric.set_billing_period_sec(srcMetric.GetBillingPeriodSec());
            *metric.mutable_labels() = srcMetric.GetLabels();

            /* TODO: support arbitrary fields in metering core */
            auto& metricFields = *metric.mutable_metric_fields()->mutable_fields();
            metricFields["version"].set_string_value(srcMetric.GetVersion());
            metricFields["schema"].set_string_value(srcMetric.GetSchema());
            metricFields["cloud_id"].set_string_value(srcMetric.GetCloudId());
            metricFields["folder_id"].set_string_value(srcMetric.GetFolderId());
            metricFields["resource_id"].set_string_value(srcMetric.GetResourceId());
            metricFields["source_id"].set_string_value(srcMetric.GetSourceId());
        };
        if (srcAcc.HasProvisioned()) {
            copyMetric(srcAcc.GetProvisioned(), *acc.mutable_provisioned());
        }
        if (srcAcc.HasOnDemand()) {
            copyMetric(srcAcc.GetOnDemand(), *acc.mutable_on_demand());
        }
        if (srcAcc.HasOvershoot()) {
            copyMetric(srcAcc.GetOvershoot(), *acc.mutable_overshoot());
        }
    }
    if (srcProps.HasReplicatedBucket()) {
        const auto& srcRepl = srcProps.GetReplicatedBucket();
        auto& repl = *props.mutable_replicated_bucket();
        if (srcRepl.HasReportIntervalMs()) {
            repl.set_report_interval_ms(srcRepl.GetReportIntervalMs());
        }
    }
}

class TCreateRateLimiterResourceRPC : public TRateLimiterControlRequest<TEvCreateRateLimiterResource> {
public:
    using TBase = TRateLimiterControlRequest<TEvCreateRateLimiterResource>;
    using TBase::TBase;
    using TBase::Handle;


    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvKesus::TEvAddQuoterResourceResult, Handle);
        default:
            return TBase::StateFunc(ev);
        }
    }

    bool ValidateRequest(Ydb::StatusIds::StatusCode& status, NYql::TIssues& issues) override {
        return ValidateResource(GetProtoRequest()->resource(), status, issues);
    }

    void SendRequest() override {
        UnsafeBecome(&TCreateRateLimiterResourceRPC::StateFunc);

        THolder<TEvKesus::TEvAddQuoterResource> req = MakeHolder<TEvKesus::TEvAddQuoterResource>();
        CopyProps(GetProtoRequest()->resource(), *req->Record.MutableResource());
        NTabletPipe::SendData(SelfId(), KesusPipeClient, req.Release(), 0);
    }

    void Handle(TEvKesus::TEvAddQuoterResourceResult::TPtr& ev) {
        ReplyFromKesusError(ev->Get()->Record.GetError());
    }
};

class TAlterRateLimiterResourceRPC : public TRateLimiterControlRequest<TEvAlterRateLimiterResource> {
public:
    using TBase = TRateLimiterControlRequest<TEvAlterRateLimiterResource>;
    using TBase::TBase;
    using TBase::Handle;

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvKesus::TEvUpdateQuoterResourceResult, Handle);
        default:
            return TBase::StateFunc(ev);
        }
    }

    bool ValidateRequest(Ydb::StatusIds::StatusCode& status, NYql::TIssues& issues) override {
        return ValidateResource(GetProtoRequest()->resource(), status, issues);
    }

    void SendRequest() override {
        UnsafeBecome(&TAlterRateLimiterResourceRPC::StateFunc);

        THolder<TEvKesus::TEvUpdateQuoterResource> req = MakeHolder<TEvKesus::TEvUpdateQuoterResource>();
        CopyProps(GetProtoRequest()->resource(), *req->Record.MutableResource());
        NTabletPipe::SendData(SelfId(), KesusPipeClient, req.Release(), 0);
    }

    void Handle(TEvKesus::TEvUpdateQuoterResourceResult::TPtr& ev) {
        ReplyFromKesusError(ev->Get()->Record.GetError());
    }
};

class TDropRateLimiterResourceRPC : public TRateLimiterControlRequest<TEvDropRateLimiterResource> {
public:
    using TBase = TRateLimiterControlRequest<TEvDropRateLimiterResource>;
    using TBase::TBase;
    using TBase::Handle;

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvKesus::TEvDeleteQuoterResourceResult, Handle);
        default:
            return TBase::StateFunc(ev);
        }
    }

    bool ValidateRequest(Ydb::StatusIds::StatusCode& status, NYql::TIssues& issues) override {
        return ValidateResourcePath(GetProtoRequest()->resource_path(), status, issues);
    }

    void SendRequest() override {
        UnsafeBecome(&TDropRateLimiterResourceRPC::StateFunc);

        THolder<TEvKesus::TEvDeleteQuoterResource> req = MakeHolder<TEvKesus::TEvDeleteQuoterResource>();
        req->Record.SetResourcePath(GetProtoRequest()->resource_path());
        NTabletPipe::SendData(SelfId(), KesusPipeClient, req.Release(), 0);
    }

    void Handle(TEvKesus::TEvDeleteQuoterResourceResult::TPtr& ev) {
        ReplyFromKesusError(ev->Get()->Record.GetError());
    }
};

class TListRateLimiterResourcesRPC : public TRateLimiterControlRequest<TEvListRateLimiterResources> {
public:
    using TBase = TRateLimiterControlRequest<TEvListRateLimiterResources>;
    using TBase::TBase;
    using TBase::Handle;

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvKesus::TEvDescribeQuoterResourcesResult, Handle);
        default:
            return TBase::StateFunc(ev);
        }
    }

    bool ValidateRequest(Ydb::StatusIds::StatusCode& status, NYql::TIssues& issues) override {
        if (const TString& path = GetProtoRequest()->resource_path()) {
            return ValidateResourcePath(path, status, issues);
        }
        return true;
    }

    void SendRequest() override {
        UnsafeBecome(&TListRateLimiterResourcesRPC::StateFunc);

        THolder<TEvKesus::TEvDescribeQuoterResources> req = MakeHolder<TEvKesus::TEvDescribeQuoterResources>();
        if (const TString& path = GetProtoRequest()->resource_path()) {
            req->Record.AddResourcePaths(path);
        }
        req->Record.SetRecursive(GetProtoRequest()->recursive());
        NTabletPipe::SendData(SelfId(), KesusPipeClient, req.Release(), 0);
    }

    void Handle(TEvKesus::TEvDescribeQuoterResourcesResult::TPtr& ev) {
        const NKikimrKesus::TKesusError& kesusError = ev->Get()->Record.GetError();
        if (kesusError.GetStatus() == Ydb::StatusIds::SUCCESS) {
            Ydb::RateLimiter::ListResourcesResult result;
            for (const auto& resource : ev->Get()->Record.GetResources()) {
                result.add_resource_paths(resource.GetResourcePath());
            }
            Request_->SendResult(result, Ydb::StatusIds::SUCCESS);
            PassAway();
        } else {
            ReplyFromKesusError(kesusError);
        }
    }
};

class TDescribeRateLimiterResourceRPC : public TRateLimiterControlRequest<TEvDescribeRateLimiterResource> {
public:
    using TBase = TRateLimiterControlRequest<TEvDescribeRateLimiterResource>;
    using TBase::TBase;
    using TBase::Handle;

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvKesus::TEvDescribeQuoterResourcesResult, Handle);
        default:
            return TBase::StateFunc(ev);
        }
    }

    bool ValidateRequest(Ydb::StatusIds::StatusCode& status, NYql::TIssues& issues) override {
        return ValidateResourcePath(GetProtoRequest()->resource_path(), status, issues);
    }

    void SendRequest() override {
        UnsafeBecome(&TDescribeRateLimiterResourceRPC::StateFunc);

        THolder<TEvKesus::TEvDescribeQuoterResources> req = MakeHolder<TEvKesus::TEvDescribeQuoterResources>();
        req->Record.AddResourcePaths(GetProtoRequest()->resource_path());
        NTabletPipe::SendData(SelfId(), KesusPipeClient, req.Release(), 0);
    }

    void Handle(TEvKesus::TEvDescribeQuoterResourcesResult::TPtr& ev) {
        const NKikimrKesus::TKesusError& kesusError = ev->Get()->Record.GetError();
        if (kesusError.GetStatus() == Ydb::StatusIds::SUCCESS) {
            Ydb::RateLimiter::DescribeResourceResult result;
            if (ev->Get()->Record.ResourcesSize() == 0) {
                this->Reply(StatusIds::INTERNAL_ERROR, "No resource properties found.", NKikimrIssues::TIssuesIds::DEFAULT_ERROR, this->ActorContext());
                return;
            }
            CopyProps(ev->Get()->Record.GetResources(0), *result.mutable_resource());
            Request_->SendResult(result, Ydb::StatusIds::SUCCESS);
            PassAway();
        } else {
            ReplyFromKesusError(kesusError);
        }
    }
};

class TAcquireRateLimiterResourceRPC : public TRateLimiterRequest<TAcquireRateLimiterResourceRPC, TEvAcquireRateLimiterResource> {
public:
    using TBase = TRateLimiterRequest<TAcquireRateLimiterResourceRPC, TEvAcquireRateLimiterResource>;
    using TBase::TBase;

    void Bootstrap(const TActorContext& ctx) {
        TBase::Bootstrap(ctx);

        UnsafeBecome(&TAcquireRateLimiterResourceRPC::StateFunc);

        Ydb::StatusIds::StatusCode status = Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
        NYql::TIssues issues;

        if (!ValidateCoordinationNodePath(status, issues)) {
            Reply(status, issues, TActivationContext::AsActorContext());
            return;
        }

        if (!ValidateRequest(status, issues)) {
            Reply(status, issues, TActivationContext::AsActorContext());
            return;
        }

        SendRequest();
    }

    // Always race when "cancel after" time is not set.
    // If "cancel after" is not set, quoter service can spend resource and say "OK", but we here reply with TIMEOUT.
    void OnOperationTimeout(const TActorContext& ctx) {
        Send(MakeQuoterServiceID(), new TEvQuota::TEvRpcTimeout(GetProtoRequest()->coordination_node_path(), GetProtoRequest()->resource_path()), 0, 0);
        TBase::OnOperationTimeout(ctx);
    }

    // Do nothing here, because quoter service replies after "cancel after" time passes.
    void OnCancelOperation(const TActorContext& ctx) {
        Y_UNUSED(ctx);
    }

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvQuota::TEvClearance, Handle);
        default:
            return TBase::StateFuncBase(ev);
        }
    }

    bool ValidateRequest(Ydb::StatusIds::StatusCode& status, NYql::TIssues& issues) {
        if (!ValidateResourcePath(GetProtoRequest()->resource_path(), status, issues)) {
            return false;
        }

        if (GetProtoRequest()->units_case() == Ydb::RateLimiter::AcquireResourceRequest::UnitsCase::UNITS_NOT_SET) {
            return false;
        }

        return true;
    }

    void SendRequest() {
        UnsafeBecome(&TAcquireRateLimiterResourceRPC::StateFunc);

        if (GetProtoRequest()->units_case() == Ydb::RateLimiter::AcquireResourceRequest::UnitsCase::kRequired) {
            SendLeaf(
                TEvQuota::TResourceLeaf(GetProtoRequest()->coordination_node_path(),
                                        GetProtoRequest()->resource_path(),
                                        GetProtoRequest()->required()));
            return;
        }

        SendLeaf(
            TEvQuota::TResourceLeaf(GetProtoRequest()->coordination_node_path(),
                                    GetProtoRequest()->resource_path(),
                                    GetProtoRequest()->used(),
                                    true));
    }

    StatusIds::StatusCode QuoterDeadlineStatusCode() {
        if (const TDuration cancelAfter = GetCancelAfter(); cancelAfter && cancelAfter < GetOperationTimeout()) {
            return StatusIds::CANCELLED;
        }
        return StatusIds::TIMEOUT;
    }

    void SendLeaf(const TEvQuota::TResourceLeaf& leaf) {
        TDuration deadline = GetOperationTimeout();
        // CancelAfter is an intelligent way to say quoter service that we can wait maximum time.
        // After that time quoter service sends EResult::Deadline.
        // It says that the system lacks the resource.
        if (const TDuration cancelAfter = GetCancelAfter(); cancelAfter && cancelAfter < deadline) {
            deadline = cancelAfter;
        }

        Send(MakeQuoterServiceID(),
            new TEvQuota::TEvRequest(TEvQuota::EResourceOperator::And, { leaf }, deadline), 0, 0);
    }

    void Handle(TEvQuota::TEvClearance::TPtr& ev) {
        switch (ev->Get()->Result) {
            case TEvQuota::TEvClearance::EResult::Success:
                Reply(StatusIds::SUCCESS, TActivationContext::AsActorContext());
                break;
            case TEvQuota::TEvClearance::EResult::UnknownResource:
                Reply(StatusIds::BAD_REQUEST, TActivationContext::AsActorContext());
                break;
            case TEvQuota::TEvClearance::EResult::Deadline:
                Reply(QuoterDeadlineStatusCode(), TActivationContext::AsActorContext());
                break;
            default:
                Reply(StatusIds::INTERNAL_ERROR, TActivationContext::AsActorContext());
        }
    }
};

} // namespace

void DoCreateRateLimiterResource(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TCreateRateLimiterResourceRPC(p.release()));
}

void DoAlterRateLimiterResource(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TAlterRateLimiterResourceRPC(p.release()));
}

void DoDropRateLimiterResource(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TDropRateLimiterResourceRPC(p.release()));
}

void DoListRateLimiterResources(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TListRateLimiterResourcesRPC(p.release()));
}

void DoDescribeRateLimiterResource(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TDescribeRateLimiterResourceRPC(p.release()));
}

void DoAcquireRateLimiterResource(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TAcquireRateLimiterResourceRPC(p.release()));
}

template<>
IActor* TEvAcquireRateLimiterResource::CreateRpcActor(NKikimr::NGRpcService::IRequestOpCtx* msg) {
    return new TAcquireRateLimiterResourceRPC(msg, true);
}

} // namespace NKikimr::NGRpcService
