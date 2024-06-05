#include "control_plane_config.h"

#include <ydb/core/fq/libs/actors/logging/log.h>
#include <ydb/core/fq/libs/control_plane_storage/control_plane_storage.h>
#include <ydb/core/fq/libs/control_plane_storage/events/events.h>
#include <ydb/core/fq/libs/control_plane_storage/util.h>
#include <ydb/core/fq/libs/quota_manager/quota_manager.h>
#include <ydb/core/fq/libs/shared_resources/db_exec.h>
#include <ydb/core/fq/libs/ydb/ydb.h>
#include <ydb/core/fq/libs/control_plane_storage/schema.h>
#include <ydb/core/fq/libs/db_schema/db_schema.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actor.h>

#include <ydb/core/fq/libs/common/util.h>
#include <ydb/library/db_pool/db_pool.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>

#include <util/generic/ptr.h>
#include <util/datetime/base.h>
#include <util/digest/multi.h>
#include <util/system/hostname.h>

namespace NFq {

using TTenantExecuter = TDbExecuter<TTenantInfo::TPtr>;
using TStateTimeExecuter = TDbExecuter<TInstant>;

class TControlPlaneConfigActor : public NActors::TActorBootstrapped<TControlPlaneConfigActor> {

    ::NFq::TYqSharedResources::TPtr YqSharedResources;
    NKikimr::TYdbCredentialsProviderFactory CredProviderFactory;
    TYdbConnectionPtr YdbConnection;
    NDbPool::TDbPool::TPtr DbPool;
    ::NMonitoring::TDynamicCounterPtr Counters;
    NConfig::TControlPlaneStorageConfig Config;
    NConfig::TComputeConfig ComputeConfig;
    TTenantInfo::TPtr TenantInfo;
    bool LoadInProgress = false;
    TDuration DbReloadPeriod;
    TString TablePathPrefix;

public:
    TControlPlaneConfigActor(const ::NFq::TYqSharedResources::TPtr& yqSharedResources, const NKikimr::TYdbCredentialsProviderFactory& credProviderFactory, const NConfig::TControlPlaneStorageConfig& config, const NConfig::TComputeConfig& computeConfig, const ::NMonitoring::TDynamicCounterPtr& counters)
        : YqSharedResources(yqSharedResources)
        , CredProviderFactory(credProviderFactory)
        , Counters(counters)
        , Config(config)
        , ComputeConfig(computeConfig)
    {
        DbReloadPeriod = GetDuration(Config.GetDbReloadPeriod(), TDuration::Seconds(3));
    }

    static constexpr char ActorName[] = "FQ_CONTROL_PLANE_CONFIG";

    void Bootstrap() {
        CPC_LOG_D("STARTING: " << SelfId());
        Become(&TControlPlaneConfigActor::StateFunc);
        if (Config.GetUseDbMapping()) {
            YdbConnection = NewYdbConnection(Config.GetStorage(), CredProviderFactory, YqSharedResources->CoreYdbDriver);
            DbPool = YqSharedResources->DbPoolHolder->GetOrCreate(static_cast<ui32>(EDbPoolId::MAIN));
            TablePathPrefix = YdbConnection->TablePathPrefix;
            Schedule(TDuration::Zero(), new NActors::TEvents::TEvWakeup());
        } else {
            TenantInfo.reset(new TTenantInfo(ComputeConfig));
            const auto& mapping = Config.GetMapping();
            for (const auto& cloudToTenant : mapping.GetCloudIdToTenantName()) {
                TenantInfo->SubjectMapping[SUBJECT_TYPE_CLOUD].emplace(cloudToTenant.GetKey(), cloudToTenant.GetValue());
                TenantInfo->TenantMapping.emplace(cloudToTenant.GetValue(), cloudToTenant.GetValue());
            }
            for (const auto& commonTenantName : mapping.GetCommonTenantName()) {
                TenantInfo->TenantMapping.emplace(commonTenantName, commonTenantName);
                TenantInfo->CommonVTenants.push_back(commonTenantName);
            }
        }
    }

private:

    STRICT_STFUNC(StateFunc,
        hFunc(NActors::TEvents::TEvWakeup, Handle);
        hFunc(TEvents::TEvCallback, [](TEvents::TEvCallback::TPtr& ev) { ev->Get()->Callback(); } );
        hFunc(TEvControlPlaneStorage::TEvGetTaskResponse, Handle); // self ping response
        hFunc(TEvControlPlaneConfig::TEvGetTenantInfoRequest, Handle);
    )

    void Handle(NActors::TEvents::TEvWakeup::TPtr&) {
        if (!LoadInProgress) {
            LoadTenantsAndMapping();
        }
        Schedule(DbReloadPeriod, new NActors::TEvents::TEvWakeup());
    }

    void Handle(TEvControlPlaneStorage::TEvGetTaskResponse::TPtr& ev) {
        if (ev->Get()->Issues) {
            CPC_LOG_E("TEvGetTaskResponse (Self Ping): " << ev->Get()->Issues.ToOneLineString());
        } else if (ev->Get()->Record.tasks().size()) {
            CPC_LOG_E("TEvGetTaskResponse (Self Ping) returned : " << ev->Get()->Record.tasks().size() << " tasks, empty list expected");
        }
    }

    void Handle(TEvControlPlaneConfig::TEvGetTenantInfoRequest::TPtr& ev) {
        if (!TenantInfo) {
            CPC_LOG_W("TEvGetTenantInfoRequest: IS NOT READY yet");
        }
        Send(ev->Sender, new TEvControlPlaneConfig::TEvGetTenantInfoResponse(TenantInfo), 0, ev->Cookie);
    }

    void LoadTenantsAndMapping() {

        LoadInProgress = true;
        TDbExecutable::TPtr executable;
        auto& executer = TTenantExecuter::Create(executable, true, [computeConfig=ComputeConfig](TTenantExecuter& executer) { executer.State.reset(new TTenantInfo(computeConfig)); } );

        executer.Read(
            [=](TTenantExecuter&, TSqlQueryBuilder& builder) {
                builder.AddText(
                    "SELECT `" TENANT_COLUMN_NAME "`, `" VTENANT_COLUMN_NAME "`, `" COMMON_COLUMN_NAME "`, `" STATE_COLUMN_NAME "`, `" STATE_TIME_COLUMN_NAME "`\n"
                    "FROM `" TENANTS_TABLE_NAME "`;\n"
                    "SELECT `" SUBJECT_TYPE_COLUMN_NAME "`, `" SUBJECT_ID_COLUMN_NAME "`, `" VTENANT_COLUMN_NAME "`\n"
                    "FROM `" MAPPINGS_TABLE_NAME "`;\n"
                );
            },
            [=](TTenantExecuter& executer, const TVector<NYdb::TResultSet>& resultSets) {

                auto& info = *executer.State;

                {
                    info.CommonVTenants.clear();
                    TResultSetParser parser(resultSets.front());
                    while (parser.TryNextRow()) {
                        auto tenant = *parser.ColumnParser(TENANT_COLUMN_NAME).GetOptionalString();
                        auto vtenantColumn = parser.ColumnParser(VTENANT_COLUMN_NAME).GetOptionalString();
                        TString vtenant = vtenantColumn ? *vtenantColumn : "";
                        auto common = *parser.ColumnParser(COMMON_COLUMN_NAME).GetOptionalBool();
                        auto state = *parser.ColumnParser(STATE_COLUMN_NAME).GetOptionalUint32();
                        auto stateTime = *parser.ColumnParser(STATE_TIME_COLUMN_NAME).GetOptionalTimestamp();

                        info.TenantState.emplace(tenant, state);
                        if (info.StateTime < stateTime) {
                            info.StateTime = stateTime;
                        }

                        if (vtenant) {
                            info.TenantMapping.emplace(vtenant, tenant);
                        }

                        if (vtenant && common) {
                            info.CommonVTenants.push_back(vtenant);
                        }
                    }
                }

                {
                    TResultSetParser parser(resultSets[1]);
                    while (parser.TryNextRow()) {
                        auto subject_type = *parser.ColumnParser(SUBJECT_TYPE_COLUMN_NAME).GetOptionalString();
                        auto subject_id = *parser.ColumnParser(SUBJECT_ID_COLUMN_NAME).GetOptionalString();
                        auto vtenant = *parser.ColumnParser(VTENANT_COLUMN_NAME).GetOptionalString();
                        info.SubjectMapping[subject_type].emplace(subject_id, vtenant);
                    }
                }
            },
            "ReadTenants", true
        ).Process(SelfId(),
            [=, this, actorSystem=NActors::TActivationContext::ActorSystem(), selfId=SelfId()](TTenantExecuter& executer) {
                if (executer.State->CommonVTenants.size()) {
                    std::sort(executer.State->CommonVTenants.begin(), executer.State->CommonVTenants.end());
                }
                bool refreshed = !this->TenantInfo || (this->TenantInfo->StateTime < executer.State->StateTime);
                auto oldInfo = this->TenantInfo;
                this->TenantInfo = executer.State;

                if (refreshed) {
                    CPC_LOG_D("LOADED TenantInfo: State CHANGED at " << this->TenantInfo->StateTime);
                } else {
                    CPC_LOG_T("LOADED TenantInfo: State NOT changed");
                }

                if (refreshed) {
                    // write ack only after all members are assigned
                    TDbExecutable::TPtr executable;
                    auto& executer = TStateTimeExecuter::Create(executable, true, nullptr);
                    executer.State = this->TenantInfo->StateTime;

                    executer.Write(
                        [=, nodeId = this->SelfId().NodeId()](TStateTimeExecuter& executer, TSqlQueryBuilder& builder) {
                            builder.AddUint32("node_id", nodeId);
                            builder.AddTimestamp("state_time", executer.State);
                            builder.AddText(
                                "UPSERT INTO `" TENANT_ACKS_TABLE_NAME "` (`" NODE_ID_COLUMN_NAME "`, `" STATE_TIME_COLUMN_NAME "`)\n"
                                "    VALUES ($node_id, $state_time);\n"
                            );
                        },
                        "WriteStateTime", true
                    );

                    if (oldInfo) {
                        executer.Process(SelfId(),
                            [this, oldInfo=oldInfo](TStateTimeExecuter&) {
                                this->ReflectTenantChanges(oldInfo);
                            }
                        );
                    }

                    Exec(DbPool, executable, TablePathPrefix).Apply([executable, actorSystem, selfId](const auto& future) {
                        actorSystem->Send(selfId, new TEvents::TEvCallback([executable, future]() {
                            auto issues = GetIssuesFromYdbStatus(executable, future);
                            if (issues) {
                                CPC_LOG_E("UpdateState in case of LoadTenantsAndMapping finished with error: " << issues->ToOneLineString());
                                // Nothing to do. We will retry it in the next Wakeup
                            }
                        }));
                    });
                }

                LoadInProgress = false;
            }
        );

        Exec(DbPool, executable, TablePathPrefix).Apply([this, executable, actorSystem=NActors::TActivationContext::ActorSystem(), selfId=SelfId()](const auto& future) {
            actorSystem->Send(selfId, new TEvents::TEvCallback([this, executable, future]() {
                auto issues = GetIssuesFromYdbStatus(executable, future);
                if (issues) {
                    CPC_LOG_E("LoadTenantsAndMapping finished with error: " << issues->ToOneLineString());
                    LoadInProgress = false;
                }
            }));
        });
    }

    void ReflectTenantChanges(TTenantInfo::TPtr oldInfo) {
        for (auto& p : TenantInfo->TenantState) {
            auto& tenant = p.first;
            auto state = p.second;
            if (oldInfo->TenantState.Value(tenant, state) != state) {
                CPC_LOG_D("Tenant " << tenant << " state CHANGED to " << state);
                switch (state) {
                case TenantState::Idle:
                    ReassignPending(tenant);
                    break;
                }
            }
        }
    }

    void ReassignPending(const TString& tenant) {
        Fq::Private::GetTaskRequest request;
        request.set_tenant(tenant);
        request.set_owner_id("Self-Ping-To-Move-Jobs");
        request.set_host(HostName());
        auto event = std::make_unique<TEvControlPlaneStorage::TEvGetTaskRequest>(std::move(request));
        event->TenantInfo = TenantInfo;
        Send(ControlPlaneStorageServiceActorId(), event.release());
    }
};

TActorId ControlPlaneConfigActorId() {
    constexpr TStringBuf name = "FQCTLCFG";
    return NActors::TActorId(0, name);
}

NActors::IActor* CreateControlPlaneConfigActor(const ::NFq::TYqSharedResources::TPtr& yqSharedResources,
                                               const NKikimr::TYdbCredentialsProviderFactory& credProviderFactory,
                                               const NConfig::TControlPlaneStorageConfig& config,
                                               const NConfig::TComputeConfig& computeConfig,
                                               const ::NMonitoring::TDynamicCounterPtr& counters) {
    return new TControlPlaneConfigActor(yqSharedResources, credProviderFactory, config, computeConfig, counters);
}

}  // namespace NFq
