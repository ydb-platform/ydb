#pragma once
#include "defs.h"

#include <ydb/library/yaml_config/yaml_config.h>

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/protos/console.pb.h>
#include <ydb/core/protos/console_base.pb.h>
#include <ydb/core/protos/console_config.pb.h>
#include <ydb/core/protos/console_tenant.pb.h>
#include <ydb/public/api/protos/ydb_cms.pb.h>
#include <ydb/public/api/protos/draft/ydb_dynamic_config.pb.h>

namespace NKikimr::NConsole {

struct TEvConsole {
    enum EEv {
        // requests
        EvCreateTenantRequest = EventSpaceBegin(TKikimrEvents::ES_CONSOLE),
        EvAlterTenantRequest,
        EvGetTenantStatusRequest,
        EvListTenantsRequest,
        EvGetConfigRequest,
        EvSetConfigRequest,
        EvConfigureRequest,
        EvGetConfigItemsRequest,
        EvGetNodeConfigItemsRequest,
        EvGetNodeConfigRequest,
        EvRemoveTenantRequest,
        EvGetOperationRequest,
        EvAddConfigSubscriptionRequest,
        EvGetConfigSubscriptionRequest,
        EvListConfigSubscriptionsRequest,
        EvRemoveConfigSubscriptionRequest,
        EvRemoveConfigSubscriptionsRequest,
        EvReplaceConfigSubscriptionsRequest,
        EvConfigNotificationRequest,
        EvNotifyOperationCompletionRequest,
        EvDescribeTenantOptionsRequest,
        EvCheckConfigUpdatesRequest,
        EvListConfigValidatorsRequest,
        EvToggleConfigValidatorRequest,
        EvConfigSubscriptionRequest,
        EvConfigSubscriptionCanceled,
        EvConfigSubscriptionNotification,
        EvUpdateTenantPoolConfig,
        EvGetLogTailRequest,
        //
        EvSetYamlConfigRequest,
        EvAddVolatileConfigRequest,
        EvRemoveVolatileConfigRequest,
        EvGetAllConfigsRequest,
        EvResolveConfigRequest,
        EvResolveAllConfigRequest,
        EvDropConfigRequest,
        EvReplaceYamlConfigRequest,
        EvGetAllMetadataRequest,
        EvGetNodeLabelsRequest,
        EvIsYamlReadOnlyRequest,

        // responses
        EvCreateTenantResponse = EvCreateTenantRequest + 1024,
        EvAlterTenantResponse,
        EvGetTenantStatusResponse,
        EvListTenantsResponse,
        EvGetConfigResponse,
        EvSetConfigResponse,
        EvConfigureResponse,
        EvGetConfigItemsResponse,
        EvGetNodeConfigItemsResponse,
        EvGetNodeConfigResponse,
        EvRemoveTenantResponse,
        EvGetOperationResponse,
        EvAddConfigSubscriptionResponse,
        EvGetConfigSubscriptionResponse,
        EvListConfigSubscriptionsResponse,
        EvRemoveConfigSubscriptionResponse,
        EvRemoveConfigSubscriptionsResponse,
        EvReplaceConfigSubscriptionsResponse,
        EvConfigNotificationResponse,
        EvNotifyOperationCompletionResponse,
        EvOperationCompletionNotification,
        EvDescribeTenantOptionsResponse,
        EvCheckConfigUpdatesResponse,
        EvListConfigValidatorsResponse,
        EvToggleConfigValidatorResponse,
        EvConfigSubscriptionResponse,
        EvConfigSubscriptionError,
        EvGetLogTailResponse,
        //
        EvSetYamlConfigResponse,
        EvAddVolatileConfigResponse,
        EvRemoveVolatileConfigResponse,
        EvGetAllConfigsResponse,
        EvResolveConfigResponse,
        EvResolveAllConfigResponse,
        EvDropConfigResponse,
        EvReplaceYamlConfigResponse,
        EvGetAllMetadataResponse,
        EvGetNodeLabelsResponse,
        EvUnauthorized,
        EvDisabled,
        EvGenericError,

        EvIsYamlReadOnlyResponse,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_CONSOLE),
                  "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_CONSOLE)");

    //////////////////////////////////////////////////
    // TENANTS MANAGEMENT
    //////////////////////////////////////////////////
    struct TEvCreateTenantRequest : public TEventShortDebugPB<TEvCreateTenantRequest, NKikimrConsole::TCreateTenantRequest, EvCreateTenantRequest> {};

    struct TEvCreateTenantResponse : public TEventShortDebugPB<TEvCreateTenantResponse, NKikimrConsole::TCreateTenantResponse, EvCreateTenantResponse> {};

    struct TEvAlterTenantRequest : public TEventShortDebugPB<TEvAlterTenantRequest, NKikimrConsole::TAlterTenantRequest, EvAlterTenantRequest> {};

    struct TEvAlterTenantResponse : public TEventShortDebugPB<TEvAlterTenantResponse, NKikimrConsole::TAlterTenantResponse, EvAlterTenantResponse> {};

    struct TEvGetTenantStatusRequest : public TEventShortDebugPB<TEvGetTenantStatusRequest, NKikimrConsole::TGetTenantStatusRequest, EvGetTenantStatusRequest> {};

    struct TEvGetTenantStatusResponse : public TEventShortDebugPB<TEvGetTenantStatusResponse, NKikimrConsole::TGetTenantStatusResponse, EvGetTenantStatusResponse> {};

    struct TEvListTenantsRequest : public TEventShortDebugPB<TEvListTenantsRequest, NKikimrConsole::TListTenantsRequest, EvListTenantsRequest> {};

    struct TEvListTenantsResponse : public TEventShortDebugPB<TEvListTenantsResponse, NKikimrConsole::TListTenantsResponse, EvListTenantsResponse> {};

    struct TEvRemoveTenantRequest : public TEventShortDebugPB<TEvRemoveTenantRequest, NKikimrConsole::TRemoveTenantRequest, EvRemoveTenantRequest> {};

    struct TEvRemoveTenantResponse : public TEventShortDebugPB<TEvRemoveTenantResponse, NKikimrConsole::TRemoveTenantResponse, EvRemoveTenantResponse> {};

    struct TEvGetOperationRequest : public TEventShortDebugPB<TEvGetOperationRequest, NKikimrConsole::TGetOperationRequest, EvGetOperationRequest> {};

    struct TEvGetOperationResponse : public TEventShortDebugPB<TEvGetOperationResponse, NKikimrConsole::TGetOperationResponse, EvGetOperationResponse> {};

    struct TEvDescribeTenantOptionsRequest : public TEventShortDebugPB<TEvDescribeTenantOptionsRequest, NKikimrConsole::TDescribeTenantOptionsRequest, EvDescribeTenantOptionsRequest> {};

    struct TEvDescribeTenantOptionsResponse : public TEventShortDebugPB<TEvDescribeTenantOptionsResponse, NKikimrConsole::TDescribeTenantOptionsResponse, EvDescribeTenantOptionsResponse> {};

    struct TEvUpdateTenantPoolConfig : public TEventShortDebugPB<TEvUpdateTenantPoolConfig, NKikimrConsole::TUpdateTenantPoolConfig, EvUpdateTenantPoolConfig> {};

    //////////////////////////////////////////////////
    // CONFIGS MANAGEMENT
    //////////////////////////////////////////////////
    struct TEvConfigureRequest : public TEventShortDebugPB<TEvConfigureRequest, NKikimrConsole::TConfigureRequest, EvConfigureRequest> {};

    struct TEvConfigureResponse : public TEventShortDebugPB<TEvConfigureResponse, NKikimrConsole::TConfigureResponse, EvConfigureResponse> {};

    struct TEvGetConfigItemsRequest : public TEventShortDebugPB<TEvGetConfigItemsRequest, NKikimrConsole::TGetConfigItemsRequest, EvGetConfigItemsRequest> {};

    struct TEvGetConfigItemsResponse : public TEventShortDebugPB<TEvGetConfigItemsResponse, NKikimrConsole::TGetConfigItemsResponse, EvGetConfigItemsResponse> {};

    struct TEvGetNodeConfigItemsRequest : public TEventShortDebugPB<TEvGetNodeConfigItemsRequest, NKikimrConsole::TGetNodeConfigItemsRequest, EvGetNodeConfigItemsRequest> {};

    struct TEvGetNodeConfigItemsResponse : public TEventShortDebugPB<TEvGetNodeConfigItemsResponse, NKikimrConsole::TGetNodeConfigItemsResponse, EvGetNodeConfigItemsResponse> {};

    struct TEvGetNodeConfigRequest : public TEventShortDebugPB<TEvGetNodeConfigRequest, NKikimrConsole::TGetNodeConfigRequest, EvGetNodeConfigRequest> {};

    struct TEvGetNodeConfigResponse : public TEventShortDebugPB<TEvGetNodeConfigResponse, NKikimrConsole::TGetNodeConfigResponse, EvGetNodeConfigResponse> {};
    //////////////////////////////////////////////////
    // NEW CONFIGS MANAGEMENT
    //////////////////////////////////////////////////
    struct TEvSetYamlConfigResponse : public TEventShortDebugPB<TEvSetYamlConfigResponse, NKikimrConsole::TSetYamlConfigResponse, EvSetYamlConfigResponse> {};

    struct TEvSetYamlConfigRequest : public TEventShortDebugPB<TEvSetYamlConfigRequest, NKikimrConsole::TSetYamlConfigRequest, EvSetYamlConfigRequest> {
        using TResponse = TEvSetYamlConfigResponse;
    };

    struct TEvReplaceYamlConfigResponse : public TEventShortDebugPB<TEvReplaceYamlConfigResponse, NKikimrConsole::TReplaceYamlConfigResponse, EvReplaceYamlConfigResponse> {};

    struct TEvReplaceYamlConfigRequest : public TEventShortDebugPB<TEvReplaceYamlConfigRequest, NKikimrConsole::TReplaceYamlConfigRequest, EvReplaceYamlConfigRequest> {
        using TResponse = TEvReplaceYamlConfigResponse;
    };

    struct TEvDropConfigResponse : public TEventShortDebugPB<TEvDropConfigResponse, NKikimrConsole::TDropConfigResponse, EvDropConfigResponse> {};

    struct TEvDropConfigRequest : public TEventShortDebugPB<TEvDropConfigRequest, NKikimrConsole::TDropConfigRequest, EvDropConfigRequest> {
        using TResponse = TEvDropConfigResponse;
    };

    struct TEvAddVolatileConfigResponse : public TEventShortDebugPB<TEvAddVolatileConfigResponse, NKikimrConsole::TAddVolatileConfigResponse, EvAddVolatileConfigResponse> {};

    struct TEvAddVolatileConfigRequest : public TEventShortDebugPB<TEvAddVolatileConfigRequest, NKikimrConsole::TAddVolatileConfigRequest, EvAddVolatileConfigRequest> {
        using TResponse = TEvAddVolatileConfigResponse;
    };

    struct TEvRemoveVolatileConfigResponse : public TEventShortDebugPB<TEvRemoveVolatileConfigResponse, NKikimrConsole::TRemoveVolatileConfigResponse, EvRemoveVolatileConfigResponse> {};

    struct TEvRemoveVolatileConfigRequest : public TEventShortDebugPB<TEvRemoveVolatileConfigRequest, NKikimrConsole::TRemoveVolatileConfigRequest, EvRemoveVolatileConfigRequest> {
        using TResponse = TEvRemoveVolatileConfigResponse;
    };

    struct TEvGetAllConfigsResponse : public TEventShortDebugPB<TEvGetAllConfigsResponse, NKikimrConsole::TGetAllConfigsResponse, EvGetAllConfigsResponse> {};

    struct TEvGetAllConfigsRequest : public TEventShortDebugPB<TEvGetAllConfigsRequest, NKikimrConsole::TGetAllConfigsRequest, EvGetAllConfigsRequest> {
        using TResponse = TEvGetAllConfigsResponse;
    };

    struct TEvIsYamlReadOnlyResponse : public TEventShortDebugPB<TEvIsYamlReadOnlyResponse, NKikimrConsole::TIsYamlReadOnlyResponse, EvIsYamlReadOnlyResponse> {};

    struct TEvIsYamlReadOnlyRequest : public TEventShortDebugPB<TEvIsYamlReadOnlyRequest, NKikimrConsole::TIsYamlReadOnlyRequest, EvIsYamlReadOnlyRequest> {
        using TResponse = TEvIsYamlReadOnlyResponse;
    };

    struct TEvGetAllMetadataResponse : public TEventShortDebugPB<TEvGetAllMetadataResponse, NKikimrConsole::TGetAllMetadataResponse, EvGetAllMetadataResponse> {};

    struct TEvGetAllMetadataRequest : public TEventShortDebugPB<TEvGetAllMetadataRequest, NKikimrConsole::TGetAllMetadataRequest, EvGetAllMetadataRequest> {
        using TResponse = TEvGetAllMetadataResponse;
    };

    struct TEvGetNodeLabelsResponse : public TEventShortDebugPB<TEvGetNodeLabelsResponse, NKikimrConsole::TGetNodeLabelsResponse, EvGetNodeLabelsResponse> {};

    struct TEvGetNodeLabelsRequest : public TEventShortDebugPB<TEvGetNodeLabelsRequest, NKikimrConsole::TGetNodeLabelsRequest, EvGetNodeLabelsRequest> {
        using TResponse = TEvGetNodeLabelsResponse;
    };

    struct TEvResolveConfigRequest : public TEventShortDebugPB<TEvResolveConfigRequest, NKikimrConsole::TResolveConfigRequest, EvResolveConfigRequest> {};

    struct TEvResolveConfigResponse : public TEventShortDebugPB<TEvResolveConfigResponse, NKikimrConsole::TResolveConfigResponse, EvResolveConfigResponse> {};

    struct TEvResolveAllConfigRequest : public TEventShortDebugPB<TEvResolveAllConfigRequest, NKikimrConsole::TResolveAllConfigRequest, EvResolveAllConfigRequest> {};

    struct TEvResolveAllConfigResponse : public TEventShortDebugPB<TEvResolveAllConfigResponse, NKikimrConsole::TResolveAllConfigResponse, EvResolveAllConfigResponse> {};

    struct TEvUnauthorized : public TEventShortDebugPB<TEvUnauthorized, NKikimrConsole::TUnauthorized, EvUnauthorized> {};

    struct TEvDisabled : public TEventShortDebugPB<TEvDisabled, NKikimrConsole::TDisabled, EvDisabled> {};

    struct TEvGenericError : public TEventShortDebugPB<TEvGenericError, NKikimrConsole::TGenericError, EvGenericError> {};

    //////////////////////////////////////////////////
    // CMS MANAGEMENT
    //////////////////////////////////////////////////
    struct TEvGetConfigRequest : public TEventShortDebugPB<TEvGetConfigRequest, NKikimrConsole::TGetConfigRequest, EvGetConfigRequest> {};

    struct TEvGetConfigResponse : public TEventShortDebugPB<TEvGetConfigResponse, NKikimrConsole::TGetConfigResponse, EvGetConfigResponse> {
        TEvGetConfigResponse(const NKikimrConsole::TConfig &config = NKikimrConsole::TConfig())
        {
            Record.MutableConfig()->CopyFrom(config);
        }
    };

    struct TEvSetConfigRequest : public TEventShortDebugPB<TEvSetConfigRequest, NKikimrConsole::TSetConfigRequest, EvSetConfigRequest> {
        TEvSetConfigRequest(const NKikimrConsole::TConfig &config = NKikimrConsole::TConfig(),
                            NKikimrConsole::TConfigItem::EMergeStrategy merge = NKikimrConsole::TConfigItem::OVERWRITE)
        {
            Record.MutableConfig()->CopyFrom(config);
            Record.SetMerge(merge);
        }
    };

    struct TEvSetConfigResponse : public TEventShortDebugPB<TEvSetConfigResponse, NKikimrConsole::TSetConfigResponse, EvSetConfigResponse> {};

    //////////////////////////////////////////////////
    // SUBSCRIPTIONS MANAGEMENT
    //////////////////////////////////////////////////
    struct TEvAddConfigSubscriptionRequest : public TEventShortDebugPB<TEvAddConfigSubscriptionRequest, NKikimrConsole::TAddConfigSubscriptionRequest, EvAddConfigSubscriptionRequest> {};

    struct TEvAddConfigSubscriptionResponse : public TEventShortDebugPB<TEvAddConfigSubscriptionResponse, NKikimrConsole::TAddConfigSubscriptionResponse, EvAddConfigSubscriptionResponse> {};

    struct TEvGetConfigSubscriptionRequest : public TEventShortDebugPB<TEvGetConfigSubscriptionRequest, NKikimrConsole::TGetConfigSubscriptionRequest, EvGetConfigSubscriptionRequest> {};

    struct TEvGetConfigSubscriptionResponse : public TEventShortDebugPB<TEvGetConfigSubscriptionResponse, NKikimrConsole::TGetConfigSubscriptionResponse, EvGetConfigSubscriptionResponse> {};

    struct TEvListConfigSubscriptionsRequest : public TEventShortDebugPB<TEvListConfigSubscriptionsRequest, NKikimrConsole::TListConfigSubscriptionsRequest, EvListConfigSubscriptionsRequest> {};

    struct TEvListConfigSubscriptionsResponse : public TEventShortDebugPB<TEvListConfigSubscriptionsResponse, NKikimrConsole::TListConfigSubscriptionsResponse, EvListConfigSubscriptionsResponse> {};

    struct TEvRemoveConfigSubscriptionRequest : public TEventShortDebugPB<TEvRemoveConfigSubscriptionRequest, NKikimrConsole::TRemoveConfigSubscriptionRequest, EvRemoveConfigSubscriptionRequest> {};

    struct TEvRemoveConfigSubscriptionResponse : public TEventShortDebugPB<TEvRemoveConfigSubscriptionResponse, NKikimrConsole::TRemoveConfigSubscriptionResponse, EvRemoveConfigSubscriptionResponse> {};

    struct TEvRemoveConfigSubscriptionsRequest : public TEventShortDebugPB<TEvRemoveConfigSubscriptionsRequest, NKikimrConsole::TRemoveConfigSubscriptionsRequest, EvRemoveConfigSubscriptionsRequest> {};

    struct TEvRemoveConfigSubscriptionsResponse : public TEventShortDebugPB<TEvRemoveConfigSubscriptionsResponse, NKikimrConsole::TRemoveConfigSubscriptionsResponse, EvRemoveConfigSubscriptionsResponse> {};

    struct TEvReplaceConfigSubscriptionsRequest : public TEventShortDebugPB<TEvReplaceConfigSubscriptionsRequest, NKikimrConsole::TReplaceConfigSubscriptionsRequest, EvReplaceConfigSubscriptionsRequest> {};

    struct TEvReplaceConfigSubscriptionsResponse : public TEventShortDebugPB<TEvReplaceConfigSubscriptionsResponse, NKikimrConsole::TReplaceConfigSubscriptionsResponse, EvReplaceConfigSubscriptionsResponse> {};

    struct TEvConfigNotificationRequest : public TEventShortDebugPB<TEvConfigNotificationRequest, NKikimrConsole::TConfigNotificationRequest, EvConfigNotificationRequest> {
        const NKikimrConfig::TAppConfig& GetConfig() const { return Record.GetConfig(); }
    };

    struct TEvConfigNotificationResponse : public TEventShortDebugPB<TEvConfigNotificationResponse, NKikimrConsole::TConfigNotificationResponse, EvConfigNotificationResponse> {
        TEvConfigNotificationResponse() {}

        TEvConfigNotificationResponse(const NKikimrConsole::TConfigNotificationRequest &request)
        {
            Record.SetSubscriptionId(request.GetSubscriptionId());
            Record.MutableConfigId()->CopyFrom(request.GetConfigId());
        }
    };

    struct TEvConfigSubscriptionRequest : public TEventShortDebugPB<TEvConfigSubscriptionRequest, NKikimrConsole::TConfigSubscriptionRequest, EvConfigSubscriptionRequest> {};

    struct TEvConfigSubscriptionResponse : public TEventShortDebugPB<TEvConfigSubscriptionResponse, NKikimrConsole::TConfigSubscriptionResponse, EvConfigSubscriptionResponse> {
        TEvConfigSubscriptionResponse() = default;

        TEvConfigSubscriptionResponse(ui64 generation, Ydb::StatusIds::StatusCode code, TString reason = TString()) {
            Record.SetGeneration(generation);
            Record.MutableStatus()->SetCode(code);
            if (reason)
                Record.MutableStatus()->SetReason(std::move(reason));
        }
    };

    struct TEvConfigSubscriptionError : public TEventShortDebugPB<TEvConfigSubscriptionError, NKikimrConsole::TConfigSubscriptionError, EvConfigSubscriptionError> {
        TEvConfigSubscriptionError() = default;

        TEvConfigSubscriptionError(Ydb::StatusIds::StatusCode code, TString reason)
        {
            Record.SetCode(code);
            Record.SetReason(std::move(reason));
        }
    };

    struct TEvConfigSubscriptionCanceled : public TEventShortDebugPB<TEvConfigSubscriptionCanceled, NKikimrConsole::TConfigSubscriptionCanceled, EvConfigSubscriptionCanceled> {
        TEvConfigSubscriptionCanceled() = default;

        explicit TEvConfigSubscriptionCanceled(ui64 generation)
        {
            Record.SetGeneration(generation);
        }
    };

    struct TEvConfigSubscriptionNotification : public TEventShortDebugPB<TEvConfigSubscriptionNotification, NKikimrConsole::TConfigSubscriptionNotification, EvConfigSubscriptionNotification> {
        TEvConfigSubscriptionNotification() = default;

        TEvConfigSubscriptionNotification(
            ui64 generation,
            NKikimrConfig::TAppConfig &&config,
            const THashSet<ui32> &affectedKinds,
            const TString &yamlConfig = {},
            const TMap<ui64, TString> &volatileYamlConfigs = {})
        {
            Record.SetGeneration(generation);
            Record.MutableConfig()->Swap(&config);
            for (ui32 kind : affectedKinds)
                Record.AddAffectedKinds(kind);

            if (!yamlConfig.empty()) {
                Record.SetYamlConfig(yamlConfig);
                for (auto &[id, config] : volatileYamlConfigs) {
                    auto *volatileConfig = Record.AddVolatileConfigs();
                    volatileConfig->SetId(id);
                    volatileConfig->SetConfig(config);
                }
            }
        }

        TEvConfigSubscriptionNotification(
            ui64 generation,
            const NKikimrConfig::TAppConfig &config,
            const THashSet<ui32> &affectedKinds,
            const TString &yamlConfig = {},
            const TMap<ui64, TString> &volatileYamlConfigs = {},
            const NKikimrConfig::TAppConfig &rawConfig = {})
        {
            Record.SetGeneration(generation);
            Record.MutableConfig()->CopyFrom(config);
            Record.MutableRawConsoleConfig()->CopyFrom(rawConfig);
            for (ui32 kind : affectedKinds)
                Record.AddAffectedKinds(kind);

            if (!yamlConfig.empty()) {
                Record.SetYamlConfig(yamlConfig);
                for (auto &[id, config] : volatileYamlConfigs) {
                    auto *volatileConfig = Record.AddVolatileConfigs();
                    volatileConfig->SetId(id);
                    volatileConfig->SetConfig(config);
                }
            }
        }
    };

    /**
     * If operation is unknown then TEvNotifyOperationCompletionResponse
     * is sent with NOT_FOUND status error.
     * If operation is not ready then subscription is stored in DB and
     * TEvNotifyOperationCompletionResponse with 'not ready' status is
     * sent. Later TEvOperationCompletionNotification is send upon
     * operation completion.
     * If operation is ready then TEvOperationCompletionNotification is
     * sent with no preceding TEvNotifyOperationCompletionResponse.
     */
    struct TEvNotifyOperationCompletionRequest : public TEventShortDebugPB<TEvNotifyOperationCompletionRequest, NKikimrConsole::TGetOperationRequest, EvNotifyOperationCompletionRequest> {};

    struct TEvNotifyOperationCompletionResponse : public TEventShortDebugPB<TEvNotifyOperationCompletionResponse, NKikimrConsole::TGetOperationResponse, EvNotifyOperationCompletionResponse> {};

    struct TEvOperationCompletionNotification : public TEventShortDebugPB<TEvOperationCompletionNotification, NKikimrConsole::TGetOperationResponse, EvOperationCompletionNotification> {};

    struct TEvCheckConfigUpdatesRequest : public TEventShortDebugPB<TEvCheckConfigUpdatesRequest, NKikimrConsole::TCheckConfigUpdatesRequest, EvCheckConfigUpdatesRequest> {};

    struct TEvCheckConfigUpdatesResponse : public TEventShortDebugPB<TEvCheckConfigUpdatesResponse, NKikimrConsole::TCheckConfigUpdatesResponse, EvCheckConfigUpdatesResponse> {};

    struct TEvListConfigValidatorsRequest : public TEventShortDebugPB<TEvListConfigValidatorsRequest, NKikimrConsole::TListConfigValidatorsRequest, EvListConfigValidatorsRequest> {};

    struct TEvListConfigValidatorsResponse : public TEventShortDebugPB<TEvListConfigValidatorsResponse, NKikimrConsole::TListConfigValidatorsResponse, EvListConfigValidatorsResponse> {};

    struct TEvToggleConfigValidatorRequest : public TEventShortDebugPB<TEvToggleConfigValidatorRequest, NKikimrConsole::TToggleConfigValidatorRequest, EvToggleConfigValidatorRequest> {};

    struct TEvToggleConfigValidatorResponse : public TEventShortDebugPB<TEvToggleConfigValidatorResponse, NKikimrConsole::TToggleConfigValidatorResponse, EvToggleConfigValidatorResponse> {};

    //////////////////////////////////////////////////
    // AUDIT
    //////////////////////////////////////////////////
    struct TEvGetLogTailRequest : public TEventPB<TEvGetLogTailRequest, NKikimrConsole::TGetLogTailRequest, EvGetLogTailRequest> {};

    struct TEvGetLogTailResponse : public TEventPB<TEvGetLogTailResponse, NKikimrConsole::TGetLogTailResponse, EvGetLogTailResponse> {};
};

IActor *CreateConsole(const TActorId &tablet, TTabletStorageInfo *info);

} // namespace NKikimr::NConsole
