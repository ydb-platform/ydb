#include "console_tenants_manager.h"
#include "console_impl.h"

namespace NKikimr::NConsole {

class TTenantsManager::TTxAlterTenant : public TTransactionBase<TTenantsManager> {
public:
    TTxAlterTenant(TEvConsole::TEvAlterTenantRequest::TPtr ev, TTenantsManager *self)
        : TBase(self)
        , Path(CanonizePath(ev->Get()->Record.GetRequest().path()))
        , Request(std::move(ev))
        , ComputationalUnitsModified(false)
    {
    }

    bool Error(Ydb::StatusIds::StatusCode code, const TString &error,
               const TActorContext &ctx)
    {
        LOG_DEBUG_S(ctx, NKikimrServices::CMS_TENANTS, "Cannot alter tenant: " << error);

        auto &operation = *Response->Record.MutableResponse()->mutable_operation();
        operation.set_ready(true);
        operation.set_status(code);
        auto issue = operation.add_issues();
        issue->set_severity(NYql::TSeverityIds::S_ERROR);
        issue->set_message(error);

        Tenant = nullptr;

        return true;
    }

    bool Execute(TTransactionContext &txc, const TActorContext &executorCtx) override
    {
        auto ctx = executorCtx.MakeFor(Self->SelfId());
        Ydb::StatusIds::StatusCode code;
        TString error;

        auto &rec = Request->Get()->Record.GetRequest();
        auto &token = Request->Get()->Record.GetUserToken();
        LOG_DEBUG_S(ctx, NKikimrServices::CMS_TENANTS, "TTxAlterTenant: "
                    << Request->Get()->Record.ShortDebugString());

        Response = new TEvConsole::TEvAlterTenantResponse;

        if (!Self->CheckAccess(token, code, error, ctx))
            return Error(code, error, ctx);

        auto path = CanonizePath(rec.path());
        Tenant = Self->GetTenant(path);
        if (!Tenant)
            return Error(Ydb::StatusIds::NOT_FOUND,
                         Sprintf("Database '%s' doesn't exist", path.data()), ctx);

        if (!Tenant->IsRunning() && !Tenant->IsConfiguring())
            return Error(Ydb::StatusIds::UNAVAILABLE,
                         Sprintf("Database '%s' is busy", path.data()), ctx);

        // Check idempotency key
        if (rec.idempotency_key() && Tenant->AlterIdempotencyKey == rec.idempotency_key()) {
            LOG_DEBUG_S(ctx, NKikimrServices::CMS_TENANTS, "Returning success due to idempotency key match");
            auto &operation = *Response->Record.MutableResponse()->mutable_operation();
            operation.set_ready(true);
            operation.set_status(Ydb::StatusIds::SUCCESS);
            Tenant = nullptr;
            return true;
        }

        // Check generation.
        if (rec.generation() && rec.generation() != Tenant->Generation)
            return Error(Ydb::StatusIds::BAD_REQUEST,
                         TStringBuilder() << "Tenant generation (" << Tenant->Generation
                         << ") doesn't match requested (" << rec.generation() << ")",
                         ctx);

        // Check added computational units.
        NewComputationalUnits = Tenant->ComputationalUnits;
        for (auto &unit : rec.computational_units_to_add()) {
            if (Tenant->SharedDomainId)
                return Error(Ydb::StatusIds::BAD_REQUEST,
                            Sprintf("Database '%s' is serverless, cannot add computational units", path.data()), ctx);

            auto &kind = unit.unit_kind();
            auto &zone = unit.availability_zone();
            ui64 count = unit.count();

            if (!Self->MakeBasicComputationalUnitCheck(kind, zone, code, error))
                return Error(code, error, ctx);

            NewComputationalUnits[std::make_pair(kind, zone)] += count;
        }

        // Check removed computational units.
        for (auto &unit : rec.computational_units_to_remove()) {
            if (Tenant->SharedDomainId)
                return Error(Ydb::StatusIds::BAD_REQUEST,
                            Sprintf("Database '%s' is serverless, cannot remove computational units", path.data()), ctx);

            auto &kind = unit.unit_kind();
            auto &zone = unit.availability_zone();
            ui64 count = unit.count();

            if (!Self->MakeBasicComputationalUnitCheck(kind, zone, code, error))
                return Error(code, error, ctx);

            auto key = std::make_pair(kind, zone);
            if (count > NewComputationalUnits[key])
                return Error(Ydb::StatusIds::BAD_REQUEST,
                             Sprintf("Not enough units of kind '%s' in zone '%s' to remove",
                                     kind.data(), zone.data()),
                             ctx);
            else if (count == NewComputationalUnits[key])
                NewComputationalUnits.erase(key);
            else
                NewComputationalUnits[key] -= count;
        }

        // Check added storage resource units.
        ui64 newGroups = 0;
        for (auto &unit : rec.storage_units_to_add()) {
            auto &kind = unit.unit_kind();

            if (Tenant->StoragePools.contains(kind)) {
                if (Tenant->StoragePools.at(kind)->Borrowed)
                    return Error(Ydb::StatusIds::BAD_REQUEST,
                                Sprintf("Pool '%s' is borrowed, cannot alter", kind.data()), ctx);
            } else if (Tenant->AreResourcesShared) {
                return Error(Ydb::StatusIds::UNSUPPORTED,
                            Sprintf("Database '%s' is shared, cannot add new storage units", path.data()), ctx);
            }

            if (Tenant->SharedDomainId)
                return Error(Ydb::StatusIds::BAD_REQUEST,
                            Sprintf("Database '%s' is serverless, cannot add storage units", path.data()), ctx);

            auto size = unit.count();

            if (!Self->MakeBasicPoolCheck(kind, size, code, error))
                return Error(code, error, ctx);

            PoolsToAdd[kind] += size;
            newGroups += size;
        }

        // Check deregistered computational units.
        for (auto &unit : rec.computational_units_to_deregister()) {
            if (Tenant->SharedDomainId)
                return Error(Ydb::StatusIds::BAD_REQUEST,
                            Sprintf("Database '%s' is serverless, cannot deregister computational units", path.data()), ctx);

            auto key = std::make_pair(unit.host(), unit.port());
            auto it = Tenant->RegisteredComputationalUnits.find(key);
            if (it == Tenant->RegisteredComputationalUnits.end())
                return Error(Ydb::StatusIds::BAD_REQUEST,
                             Sprintf("Cannot deregister unknown unit %s:%" PRIu32,
                                     key.first.data(), key.second),
                             ctx);
            UnitsToDeregister.insert(key);
        }

        // Check registered computational units.
        for (auto &unit : rec.computational_units_to_register()) {
            if (Tenant->SharedDomainId)
                return Error(Ydb::StatusIds::BAD_REQUEST,
                            Sprintf("Database '%s' is serverless, cannot register computational units", path.data()), ctx);

            auto key = std::make_pair(unit.host(), unit.port());
            auto it1 = Tenant->RegisteredComputationalUnits.find(key);
            if (it1 != Tenant->RegisteredComputationalUnits.end()) {
                if (it1->second.Kind != unit.unit_kind())
                    return Error(Ydb::StatusIds::BAD_REQUEST,
                                 Sprintf("Computational unit %s:%" PRIu32 " is already registered with another kind",
                                         key.first.data(), key.second),
                                 ctx);
            } else {
                auto it2 = UnitsToRegister.find(key);
                if (it2 != UnitsToRegister.end()
                    && it2->second.Kind != unit.unit_kind())
                    return Error(Ydb::StatusIds::BAD_REQUEST,
                                 Sprintf("Computational unit %s:%" PRIu32 " is registered with different kind",
                                         key.first.data(), key.second),
                                 ctx);
                UnitsToRegister[key] = TAllocatedComputationalUnit{key.first, key.second, unit.unit_kind()};
            }
        }

        // Check quotas.
        if (rec.computational_units_to_add_size()
            && (!Tenant->CheckComputationalUnitsQuota(NewComputationalUnits, code, error)
                || !Self->CheckComputationalUnitsQuota(NewComputationalUnits, Tenant, code, error)))
            return Error(code, error, ctx);
        if (newGroups && !Tenant->CheckStorageUnitsQuota(code, error, newGroups))
            return Error(code, error, ctx);

        // Check database quotas.
        if (rec.has_database_quotas()) {
            const auto& quotas = rec.database_quotas();
            auto hardQuota = quotas.data_size_hard_quota();
            auto softQuota = quotas.data_size_soft_quota();
            if (hardQuota && softQuota && hardQuota < softQuota) {
                return Error(Ydb::StatusIds::BAD_REQUEST, "Data size soft quota cannot be larger than hard quota", ctx);
            }
        }

        // Check scale recommender policies.
        if (rec.has_scale_recommender_policies()) {
            if (!Self->FeatureFlags.GetEnableScaleRecommender()) {
                return Error(Ydb::StatusIds::UNSUPPORTED, "Feature flag EnableScaleRecommender is off", ctx);
            }
            
            const auto& policies = rec.scale_recommender_policies();
            if (policies.policies().size() > 1) {
                return Error(Ydb::StatusIds::BAD_REQUEST, "Currently, no more than one policy is supported at a time", ctx);
            }

            if (!policies.policies().empty()) {
                using enum Ydb::Cms::ScaleRecommenderPolicies_ScaleRecommenderPolicy_TargetTrackingPolicy::TargetCase;
                using enum Ydb::Cms::ScaleRecommenderPolicies_ScaleRecommenderPolicy::PolicyCase;

                const auto& policy = policies.policies()[0];
                switch (policy.GetPolicyCase()) {
                    case kTargetTrackingPolicy: {
                        const auto& targetTracking = policy.target_tracking_policy();
                        switch (targetTracking.GetTargetCase()) {
                            case kAverageCpuUtilizationPercent: {
                                auto cpuUtilization = targetTracking.average_cpu_utilization_percent();
                                if (cpuUtilization < 10 || cpuUtilization > 90) {
                                    return Error(Ydb::StatusIds::BAD_REQUEST, "Average CPU utilization target must be from 10% to 90%", ctx);
                                }
                                break;
                            }
                            case TARGET_NOT_SET:
                                return Error(Ydb::StatusIds::BAD_REQUEST, "Target type for target tracking policy is not set", ctx);
                            default:
                                return Error(Ydb::StatusIds::BAD_REQUEST, "Unsupported target type for target tracking policy", ctx);
                            }
                        break;
                    }
                    case POLICY_NOT_SET:
                        return Error(Ydb::StatusIds::BAD_REQUEST, "Policy type is not set", ctx);
                    default:
                        return Error(Ydb::StatusIds::BAD_REQUEST, "Unsupported policy type", ctx);
                }
            }
        }
        
        // Check attributes.
        THashSet<TString> attrNames;
        for (const auto& [key, value] : rec.alter_attributes()) {
            if (!key)
               return Error(Ydb::StatusIds::BAD_REQUEST,
                             "Attribute name shouldn't be empty", ctx);
            if (attrNames.contains(key))
                return Error(Ydb::StatusIds::BAD_REQUEST,
                             Sprintf("Multiple attributes with name '%s'", key.data()), ctx);
            attrNames.insert(key);
        }

        THashMap<TString, ui64> attributeMap;
        for (ui64 i = 0 ; i < Tenant->Attributes.UserAttributesSize(); i++) {
            bool res = attributeMap.emplace(Tenant->Attributes.GetUserAttributes(i).GetKey(), i).second;
            if (!res)
                return Error(Ydb::StatusIds::INTERNAL_ERROR,
                             "Unexpected duplicate attribute found in CMS local db", ctx);
        }

        // Apply computational units changes.
        if (rec.computational_units_to_add_size() || rec.computational_units_to_remove_size()) {
            ComputationalUnitsModified = true;
            for (auto &pr : Tenant->ComputationalUnits) {
                if (!NewComputationalUnits.contains(pr.first))
                    Self->DbRemoveComputationalUnit(Tenant, pr.first.first, pr.first.second, txc, ctx);
            }
            for (auto &pr : NewComputationalUnits)
                Self->DbUpdateComputationalUnit(Tenant, pr.first.first, pr.first.second, pr.second, txc, ctx);
        }

        // Apply storage units changes.
        for (auto &pr : PoolsToAdd) {
            TStoragePool::TPtr pool;
            auto &kind = pr.first;
            auto size = pr.second;
            if (Tenant->StoragePools.contains(kind)) {
                auto cur = Tenant->StoragePools.at(kind);
                Y_ABORT_UNLESS(!cur->Borrowed);
                pool = new TStoragePool(*cur);
                pool->AddRequiredGroups(size);
                pool->State = TStoragePool::NOT_UPDATED;
            } else {
                Y_ABORT_UNLESS(!Tenant->AreResourcesShared);
                pool = Self->MakeStoragePool(Tenant, kind, size);
            }

            Self->DbUpdatePool(Tenant, pool, txc, ctx);
        }

        // Apply registered units changes.
        for (auto &key : UnitsToDeregister)
            Self->DbRemoveRegisteredUnit(Tenant, key.first, key.second, txc, ctx);
        for (auto &pr : UnitsToRegister) {
            auto &unit = pr.second;
            Self->DbUpdateRegisteredUnit(Tenant, unit.Host, unit.Port, unit.Kind, txc, ctx);
        }

        bool updateSubdomainVersion = false;

        if (rec.has_schema_operation_quotas()) {
            SchemaOperationQuotas.ConstructInPlace(rec.schema_operation_quotas());
            Self->DbUpdateSchemaOperationQuotas(Tenant, *SchemaOperationQuotas, txc, ctx);
            updateSubdomainVersion = true;
        }

        if (rec.has_database_quotas()) {
            DatabaseQuotas.ConstructInPlace(rec.database_quotas());
            Self->DbUpdateDatabaseQuotas(Tenant, *DatabaseQuotas, txc, ctx);
            updateSubdomainVersion = true;
        }

        if (rec.has_scale_recommender_policies()) {
            ScaleRecommenderPolicies.ConstructInPlace(rec.scale_recommender_policies());
            Self->DbUpdateScaleRecommenderPolicies(Tenant, *ScaleRecommenderPolicies, txc, ctx);
        }

        if (rec.idempotency_key() || Tenant->AlterIdempotencyKey) {
            Tenant->AlterIdempotencyKey = rec.idempotency_key();
            Self->DbUpdateTenantAlterIdempotencyKey(Tenant, Tenant->AlterIdempotencyKey, txc, ctx);
        }

        if (!rec.alter_attributes().empty()) {
            for (const auto& [key, value] : rec.alter_attributes()) {
                const auto it = attributeMap.find(key);
                if (it != attributeMap.end()) {
                    if (value) {
                        Tenant->Attributes.MutableUserAttributes(it->second)->SetValue(value);
                    } else {
                        Tenant->Attributes.MutableUserAttributes(it->second)->ClearValue();
                    }
                } else {
                    auto &r = *Tenant->Attributes.AddUserAttributes();
                    r.SetKey(key);
                    if (value) {
                        r.SetValue(value);
                    }
                }
            }
            Self->DbUpdateTenantUserAttributes(Tenant, Tenant->Attributes, txc, ctx);
            updateSubdomainVersion = true;
        }

        if (updateSubdomainVersion) {
            SubdomainVersion = Tenant->SubdomainVersion + 1;
            Self->DbUpdateSubdomainVersion(Tenant, *SubdomainVersion, txc, ctx);
        }

        Self->DbUpdateTenantGeneration(Tenant, Tenant->Generation + 1, txc, ctx);

        auto &operation = *Response->Record.MutableResponse()->mutable_operation();
        operation.set_ready(true);
        operation.set_status(Ydb::StatusIds::SUCCESS);

        return true;
    }

    void Complete(const TActorContext &executorCtx) override
    {
        auto ctx = executorCtx.MakeFor(Self->SelfId());
        LOG_DEBUG(ctx, NKikimrServices::CMS_TENANTS, "TTxAlterTenant Complete");

        Y_ABORT_UNLESS(Response);
        Self->Counters.Inc(Response->Record.GetResponse().operation().status(),
                           COUNTER_ALTER_RESPONSES);

        LOG_TRACE_S(ctx, NKikimrServices::CMS_TENANTS, "Send: " << Response->ToString());
        ctx.Send(Request->Sender, Response.Release(), 0, Request->Cookie);

        if (Tenant) {
            if (ComputationalUnitsModified) {
                Self->SlotStats.DeallocateSlots(Tenant->Slots);
                Self->Counters.RemoveUnits(Tenant->ComputationalUnits);

                Tenant->ComputationalUnits = NewComputationalUnits;
                Tenant->ParseComputationalUnits(Self->Config);

                Self->SlotStats.AllocateSlots(Tenant->Slots);
                Self->Counters.AddUnits(Tenant->ComputationalUnits);
            }
            for (auto &key : UnitsToDeregister) {
                Self->Counters.Dec(Tenant->RegisteredComputationalUnits.at(key).Kind,
                                   COUNTER_REGISTERED_UNITS);
                Tenant->RegisteredComputationalUnits.erase(key);
            }
            for (auto &pr : UnitsToRegister) {
                Self->Counters.Inc(pr.second.Kind, COUNTER_REGISTERED_UNITS);
                Tenant->RegisteredComputationalUnits[pr.first] = pr.second;
            }
            for (auto &pr : PoolsToAdd) {
                TStoragePool::TPtr pool;
                auto &kind = pr.first;
                auto size = pr.second;
                if (Tenant->StoragePools.contains(kind)) {
                    pool = Tenant->StoragePools.at(kind);
                    pool->AddRequiredGroups(size);
                    pool->State = TStoragePool::NOT_UPDATED;
                } else {
                    pool = Self->MakeStoragePool(Tenant, kind, size);
                    Tenant->StoragePools.emplace(std::make_pair(kind, pool));
                }

                Self->Counters.Inc(kind, COUNTER_REQUESTED_STORAGE_UNITS, size);
            }
            if (SchemaOperationQuotas) {
                Tenant->SchemaOperationQuotas.ConstructInPlace(*SchemaOperationQuotas);
            }
            if (DatabaseQuotas) {
                Tenant->DatabaseQuotas.ConstructInPlace(*DatabaseQuotas);
            }
            if (ScaleRecommenderPolicies) {
                Tenant->ScaleRecommenderPolicies.ConstructInPlace(*ScaleRecommenderPolicies);
                Tenant->ScaleRecommenderPoliciesConfirmed = false;
            }
            if (SubdomainVersion) {
                Tenant->SubdomainVersion = *SubdomainVersion;
            }

            ++Tenant->Generation;

            Self->ProcessTenantActions(Tenant, ctx);
        }

        Self->TxProcessor->TxCompleted(this, ctx);
    }

private:
    TString Path;
    TEvConsole::TEvAlterTenantRequest::TPtr Request;
    TAutoPtr<TEvConsole::TEvAlterTenantResponse> Response;
    THashMap<std::pair<TString, TString>, ui64> NewComputationalUnits;
    THashMap<std::pair<TString, ui32>, TAllocatedComputationalUnit> UnitsToRegister;
    THashSet<std::pair<TString, ui32>> UnitsToDeregister;
    THashMap<TString, ui64> PoolsToAdd;
    TMaybe<Ydb::Cms::SchemaOperationQuotas> SchemaOperationQuotas;
    TMaybe<Ydb::Cms::DatabaseQuotas> DatabaseQuotas;
    TMaybe<Ydb::Cms::ScaleRecommenderPolicies> ScaleRecommenderPolicies;
    TMaybe<ui64> SubdomainVersion;
    bool ComputationalUnitsModified;
    TTenant::TPtr Tenant;
};

ITransaction *TTenantsManager::CreateTxAlterTenant(TEvConsole::TEvAlterTenantRequest::TPtr &ev)
{
    return new TTxAlterTenant(ev, this);
}

} // namespace NKikimr::NConsole
