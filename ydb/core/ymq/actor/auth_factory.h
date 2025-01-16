#pragma once

#include "actor.h"
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/core/ymq/base/action.h>
#include <ydb/core/ymq/base/counters.h>
#include <ydb/library/http_proxy/authorization/signature.h>
#include <ydb/core/base/appdata.h>
#include <ydb/public/sdk/cpp/client/ydb_types/credentials/credentials.h>

namespace NKikimr::NSQS {

struct TAuthActorData {
    // Used by both private and public API

    enum ESqsRequestFormat {
        Xml = 0,
        Json
    };

    THolder<NKikimrClient::TSqsRequest> SQSRequest;
    THolder<IReplyCallback> HTTPCallback;
    std::function<void(TString)> UserSidCallback;
    bool EnableQueueLeader;
    // Used by private API only
    EAction Action;
    ui32 ExecutorPoolID;
    TStringBuf CloudID;
    TStringBuf ResourceID;
    TCloudAuthCounters * Counters; //nullptr when constructed from public API
    THolder<TAwsRequestSignV4> AWSSignature;
    // Used only by private API for which AWSSignature is empty.

    TStringBuf IAMToken;
    TStringBuf FolderID;

    ESqsRequestFormat RequestFormat = Xml;
    TActorId Requester;
};

/**
 * 1. Initializes and registers authorization proxy actors.
 * 2. Creates a credentials provider factory which in turn
 *   registers a concrete authorization method (oauth/iam)
 *   depending on input parameters.
 *
 * Handles both internal (Yandex cloud, kikimr/yndx/sqs) and open
 * source (ydb/core/ymq/actor) versions.
 *
 * @note An ICredentialsProviderFactory abstraction layer is
 *   unnecessary here as we could register authorization methods directly
 *   in IAuthFactory method. Unfortunately, ICredentialsProviderFactory
 *   is part of public API, so removing or changing it would take some time.
 *   TODO(KIKIMR-13892)
 */
class IAuthFactory {
public:
    using TSqsConfig = NKikimrConfig::TSqsConfig;
    using TCredentialsFactoryPtr = std::shared_ptr<NYdb::ICredentialsProviderFactory>;

    virtual void Initialize(
        NActors::TActorSystemSetup::TLocalServices& services,
        const TAppData& appData,
        const TSqsConfig& config) = 0;

    virtual void RegisterAuthActor(NActors::TActorSystem& system, TAuthActorData&& data) = 0;

    virtual TCredentialsFactoryPtr CreateCredentialsProviderFactory(const TSqsConfig& config) = 0;

    virtual ~IAuthFactory() = default;
};

// Open source implementation. Supports oAuth only.
class TAuthFactory : public IAuthFactory {
public:
    inline void Initialize(
        NActors::TActorSystemSetup::TLocalServices&,
        const TAppData&,
        const TSqsConfig& config) final {
        Y_ABORT_UNLESS(!config.GetYandexCloudMode());
    }

    void RegisterAuthActor(NActors::TActorSystem& system, TAuthActorData&& data) final;

    TCredentialsFactoryPtr CreateCredentialsProviderFactory(const TSqsConfig& config) final;
};
}
