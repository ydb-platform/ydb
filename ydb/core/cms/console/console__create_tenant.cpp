#include "console_tenants_manager.h"

namespace NKikimr::NConsole {

using namespace NOperationId;

class TTenantsManager::TTxCreateTenant : public TTransactionBase<TTenantsManager> {
    static const Ydb::Cms::Resources& GetResources(const Ydb::Cms::CreateDatabaseRequest& request) {
        switch (request.resources_kind_case()) {
        case Ydb::Cms::CreateDatabaseRequest::kResources:
            return request.resources();
        case Ydb::Cms::CreateDatabaseRequest::kSharedResources:
            return request.shared_resources();
        default:
            Y_FAIL_S("There is no resources: " << static_cast<ui32>(request.resources_kind_case()));
        }
    }

public:
    TTxCreateTenant(TEvConsole::TEvCreateTenantRequest::TPtr ev, TTenantsManager *self)
        : TBase(self)
        , Path(CanonizePath(ev->Get()->Record.GetRequest().path()))
        , Request(std::move(ev))
    {
    }

    bool Error(Ydb::StatusIds::StatusCode code, const TString &error,
               const TActorContext &ctx)
    {
        LOG_DEBUG_S(ctx, NKikimrServices::CMS_TENANTS, "Cannot create tenant: " << error);

        auto &operation = *Response->Record.MutableResponse()->mutable_operation();
        operation.set_ready(true);
        operation.set_status(code);
        auto issue = operation.add_issues();
        issue->set_severity(NYql::TSeverityIds::S_ERROR);
        issue->set_message(error);

        Tenant = nullptr;

        return true;
    }

    bool Pending(const TTenant::TPtr& tenant) {
        Ydb::TOperationId id = Self->MakeOperationId(tenant, TTenant::CREATE);
        auto &operation = *Response->Record.MutableResponse()->mutable_operation();
        operation.set_ready(false);
        operation.set_id(ProtoToString(id));
        return true;
    }

    bool Pending(const TString &path, ui64 txId) {
        Ydb::TOperationId id = Self->MakeOperationId(path, txId, TTenant::CREATE);
        auto &operation = *Response->Record.MutableResponse()->mutable_operation();
        operation.set_ready(false);
        operation.set_id(ProtoToString(id));
        return true;
    }

    bool Execute(TTransactionContext &txc, const TActorContext &executorCtx) override
    {
        auto ctx = executorCtx.MakeFor(Self->SelfId());
        Ydb::StatusIds::StatusCode code;
        TString error;

        auto &rec = Request->Get()->Record.GetRequest();
        auto &token = Request->Get()->Record.GetUserToken();
        LOG_DEBUG_S(ctx, NKikimrServices::CMS_TENANTS, "TTxCreateTenant: "
                    << Request->Get()->Record.ShortDebugString());

        Response = new TEvConsole::TEvCreateTenantResponse;

        if (!Self->CheckAccess(token, code, error, ctx))
            return Error(code, error, ctx);

        auto path = rec.path();
        if (!CheckDbPath(path, Self->Domain->Name, error))
            return Error(Ydb::StatusIds::BAD_REQUEST, error, ctx);
        path = CanonizePath(path);

        if (auto tenant = Self->GetTenant(path)) {
            if (rec.idempotency_key() && tenant->CreateIdempotencyKey == rec.idempotency_key()) {
                return Pending(tenant);
            } else if (tenant->IsRemoving()) {
                return Error(Ydb::StatusIds::PRECONDITION_FAILED,
                             Sprintf("Database '%s' is removing", path.data()), ctx);
            } else {
                return Error(Ydb::StatusIds::ALREADY_EXISTS,
                             Sprintf("Database '%s' already exists", path.data()), ctx);
            }
        }

        if (auto it = Self->RemovedTenants.find(path); it != Self->RemovedTenants.end()) {
            if (rec.idempotency_key() && it->second.CreateIdempotencyKey == rec.idempotency_key()) {
                return Pending(path, it->second.TxId);
            }
        }

        if (Self->Config.TenantsQuota
            && Self->Tenants.size() >= Self->Config.TenantsQuota) {
            LOG_NOTICE_S(ctx, NKikimrServices::CMS_TENANTS,
                         "Tenants quota is exceeded (" << Self->Tenants.size()
                         << "/" << Self->Config.TenantsQuota << ")");
            Self->Counters.Inc(COUNTER_TENANTS_QUOTA_EXCEEDED);
            return Error(Ydb::StatusIds::UNAVAILABLE,
                         "Tenants quota is exceeded", ctx);
        }

        // Check attributes.
        THashSet<TString> attrNames;
        for (const auto& [key, value] : rec.attributes()) {
            if (!key)
                return Error(Ydb::StatusIds::BAD_REQUEST,
                             "Attribute name shouldn't be empty", ctx);
            if (!value)
                return Error(Ydb::StatusIds::BAD_REQUEST,
                             "Attribute value shouldn't be empty", ctx);
            if (attrNames.contains(key))
                return Error(Ydb::StatusIds::BAD_REQUEST,
                             Sprintf("Multiple attributes with name '%s'", key.data()), ctx);
            attrNames.insert(key);
        }

        Tenant = new TTenant(path, TTenant::CREATING_POOLS, token);

        Tenant->IsExternalSubdomain = Self->FeatureFlags.GetEnableExternalSubdomains();
        Tenant->IsExternalHive = Self->FeatureFlags.GetEnableExternalHive();
        Tenant->IsExternalSysViewProcessor = Self->FeatureFlags.GetEnableSystemViews();
        Tenant->IsExternalStatisticsAggregator = Self->FeatureFlags.GetEnableStatistics();
        Tenant->IsExternalBackupController = Self->FeatureFlags.GetEnableBackupService();
        Tenant->IsGraphShardEnabled = Self->FeatureFlags.GetEnableGraphShard();

        if (rec.options().disable_external_subdomain()) {
            Tenant->IsExternalSubdomain = false;
        }

        if (rec.options().plan_resolution()) {
            Tenant->PlanResolution = rec.options().plan_resolution();
        }

        if (rec.options().disable_tx_service()) {
            Tenant->Coordinators = 0;
            Tenant->Mediators = 0;
            Tenant->PlanResolution = 0;
            Tenant->TimeCastBucketsPerMediator = 0;
            Tenant->IsExternalSubdomain = false;
            Tenant->IsExternalHive = false;
            Tenant->IsExternalSysViewProcessor = false;
            Tenant->IsExternalStatisticsAggregator = false;
            Tenant->IsExternalBackupController = false;
        }

        Tenant->IsExternalHive &= Tenant->IsExternalSubdomain; // external hive without external sub domain is pointless
        Tenant->IsExternalSysViewProcessor &= Tenant->IsExternalSubdomain;
        Tenant->IsExternalStatisticsAggregator &= Tenant->IsExternalSubdomain;
        Tenant->IsExternalBackupController &= Tenant->IsExternalSubdomain;

        Tenant->StorageUnitsQuota = Self->Config.DefaultStorageUnitsQuota;
        Tenant->ComputationalUnitsQuota = Self->Config.DefaultComputationalUnitsQuota;

        for (const auto& [key, value] : rec.attributes()) {
            auto &r = *Tenant->Attributes.AddUserAttributes();
            r.SetKey(key);
            r.SetValue(value);
        }

        switch (rec.resources_kind_case()) {
        case Ydb::Cms::CreateDatabaseRequest::kSharedResources:
            if (rec.options().disable_tx_service()) {
                return Error(Ydb::StatusIds::BAD_REQUEST,
                    "Cannot create shared database with no tx service enabled", ctx);
            }

            Tenant->AreResourcesShared = true;
            // fallthrough to normal resources handling
            [[fallthrough]];

        case Ydb::Cms::CreateDatabaseRequest::kResources:
            for (auto &unit : GetResources(rec).storage_units()) {
                auto &kind = unit.unit_kind();
                auto size = unit.count();

                if (Tenant->StoragePools.contains(kind)) {
                    auto pool = Tenant->StoragePools.at(kind);
                    pool->AddRequiredGroups(size);
                } else {
                    if (!Self->MakeBasicPoolCheck(kind, size, code, error))
                        return Error(code, error, ctx);

                    auto poolName = Tenant->MakeStoragePoolName(kind);
                    auto &config = Self->Domain->StoragePoolTypes.at(kind);
                    TStoragePool::TPtr pool = new TStoragePool(poolName, size, kind, config, false);
                    Tenant->StoragePools.emplace(std::make_pair(kind, pool));
                }
            }

            if (Tenant->StoragePools.empty())
                return Error(Ydb::StatusIds::BAD_REQUEST,
                             "No storage units specified.", ctx);

            for (auto &unit : GetResources(rec).computational_units()) {
                auto &kind = unit.unit_kind();
                auto &zone = unit.availability_zone();
                ui64 count = unit.count();

                if (!Self->MakeBasicComputationalUnitCheck(kind, zone, code, error))
                    return Error(code, error, ctx);

                Tenant->ComputationalUnits[std::make_pair(kind, zone)] += count;
            }

            // Check tenant quotas.
            if (!Tenant->CheckQuota(code, error))
                return Error(code, error, ctx);

            // Check cluster quotas.
            if (!Self->CheckComputationalUnitsQuota(Tenant->ComputationalUnits, code, error))
                return Error(code, error, ctx);

            Tenant->ParseComputationalUnits(Self->Config);
            break;

        case Ydb::Cms::CreateDatabaseRequest::kServerlessResources:
            Tenant->IsExternalStatisticsAggregator = false;

            if (!Tenant->IsExternalSubdomain) {
                return Error(Ydb::StatusIds::PRECONDITION_FAILED,
                    "Cannot create serverless database unless external subdomain is enabled", ctx);
            }

            if (rec.options().disable_tx_service()) {
                return Error(Ydb::StatusIds::BAD_REQUEST,
                    "Cannot create serverless database with no tx service enabled", ctx);
            }

            if (const TString& sharedDbPath = rec.serverless_resources().shared_database_path()) {
                if (auto tenant = Self->GetTenant(CanonizePath(sharedDbPath))) {
                    if (!tenant->AreResourcesShared) {
                        return Error(Ydb::StatusIds::PRECONDITION_FAILED,
                            TStringBuilder() << "Database is not shared: " << sharedDbPath, ctx);
                    }

                    if (!tenant->IsRunning()) {
                        return Error(Ydb::StatusIds::PRECONDITION_FAILED,
                            TStringBuilder() << "Database is not running: " << sharedDbPath, ctx);
                    }

                    Y_ABORT_UNLESS(tenant->DomainId);
                    Tenant->SharedDomainId = tenant->DomainId;
                    tenant->HostedTenants.emplace(Tenant);

                    Tenant->IsExternalHive = false;
                    Tenant->IsGraphShardEnabled = false;
                    Tenant->Coordinators = 1;
                    Tenant->SlotsAllocationConfirmed = true;
                } else {
                    return Error(Ydb::StatusIds::BAD_REQUEST,
                        TStringBuilder() << "Database not exists: " << sharedDbPath, ctx);
                }
            } else {
                return Error(Ydb::StatusIds::BAD_REQUEST, "Empty database name", ctx);
            }
            break;

        default:
            return Error(Ydb::StatusIds::BAD_REQUEST, "Unknown resources kind", ctx);
        }

        if (rec.has_schema_operation_quotas()) {
            Tenant->SchemaOperationQuotas.ConstructInPlace(rec.schema_operation_quotas());
        }

        if (rec.has_database_quotas()) {
            const auto& quotas = rec.database_quotas();
            auto hardQuota = quotas.data_size_hard_quota();
            auto softQuota = quotas.data_size_soft_quota();
            if (hardQuota && softQuota && hardQuota < softQuota) {
                return Error(Ydb::StatusIds::BAD_REQUEST,
                    TStringBuilder() << "Overall data size soft quota (" << softQuota << ")"
                                     << " of the database " << path
                                     << " must be less than or equal to the hard quota (" << hardQuota << ")",
                    ctx
                );
            }
            for (const auto& storageUnitQuota : quotas.storage_quotas()) {
                const auto unitHardQuota = storageUnitQuota.data_size_hard_quota();
                const auto unitSoftQuota = storageUnitQuota.data_size_soft_quota();
                if (unitHardQuota && unitSoftQuota && unitHardQuota < unitSoftQuota) {
                    return Error(Ydb::StatusIds::BAD_REQUEST,
                        TStringBuilder() << "Data size soft quota (" << unitSoftQuota << ")"
                                         << " for a " << storageUnitQuota.unit_kind() << " storage unit "
                                         << " of the database " << path
                                         << " must be less than or equal to"
                                         << " the corresponding hard quota (" << unitHardQuota << ")",
                        ctx
                    );
                }

            }
            Tenant->DatabaseQuotas.ConstructInPlace(quotas);
        }

        if (rec.idempotency_key()) {
            Tenant->CreateIdempotencyKey = rec.idempotency_key();
        }

        Tenant->TxId = ctx.Now().GetValue();
        Tenant->Generation = 1;

        LOG_DEBUG_S(ctx, NKikimrServices::CMS_TENANTS,
                    "Add tenant " << path << " (txid = " << Tenant->TxId << ")");

        Self->DbAddTenant(Tenant, txc, ctx);

        return Pending(Tenant);
    }

    void Complete(const TActorContext &executorCtx) override
    {
        auto ctx = executorCtx.MakeFor(Self->SelfId());
        LOG_DEBUG(ctx, NKikimrServices::CMS_TENANTS, "TTxCreateTenant Complete");

        Y_ABORT_UNLESS(Response);

        if (Response->Record.GetResponse().operation().status())
            Self->Counters.Inc(Response->Record.GetResponse().operation().status(),
                               COUNTER_CREATE_RESPONSES);

        LOG_TRACE_S(ctx, NKikimrServices::CMS_TENANTS, "Send: " << Response->ToString());
        ctx.Send(Request->Sender, Response.Release(), 0, Request->Cookie);

        if (Tenant) {
            Self->AddTenant(Tenant);
            Self->ProcessTenantActions(Tenant, ctx);
        }

        Self->TxProcessor->TxCompleted(this, ctx);
    }

private:
    TString Path;
    TEvConsole::TEvCreateTenantRequest::TPtr Request;
    TAutoPtr<TEvConsole::TEvCreateTenantResponse> Response;
    TTenant::TPtr Tenant;
};

ITransaction *TTenantsManager::CreateTxCreateTenant(TEvConsole::TEvCreateTenantRequest::TPtr &ev)
{
    return new TTxCreateTenant(ev, this);
}

} // namespace NKikimr::NConsole
