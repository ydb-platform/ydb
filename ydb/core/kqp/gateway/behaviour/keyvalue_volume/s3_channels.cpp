#include "s3_channels.h"

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/path.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/base/tabletid.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/kqp/federated_query/actors/kqp_federated_query_actors.h>
#include <ydb/core/kqp/provider/yql_kikimr_gateway.h>
#include <ydb/core/protos/bind_channel_storage_pool.pb.h>
#include <ydb/core/protos/blob_depot_config.pb.h>
#include <ydb/core/protos/blobstorage_config.pb.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/tiering/tier/s3_uri.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <util/string/builder.h>

namespace NKikimr::NKqp {

namespace {

using TYqlConclusionStatus = NMetadata::NModifications::IOperationsManager::TYqlConclusionStatus;
using TAsyncStatus = NThreading::TFuture<TYqlConclusionStatus>;
using TExternalContext = NMetadata::NModifications::IOperationsManager::TExternalModificationContext;

constexpr TStringBuf ObjectStorageSourceType = "ObjectStorage";

// The depot has to report WORKING before the volume may bind a channel to its pool, so the DDL waits for Hive to
// start the tablet. The wait is bounded to keep a failing Hive from hanging the query forever.
constexpr auto PollPeriod = TDuration::MilliSeconds(500);
constexpr auto AllocationTimeout = TDuration::Seconds(120);

struct TEvPrivate {
    enum EEv {
        EvSecretsResolved = EventSpaceBegin(TEvents::ES_PRIVATE),
        EvPoll,
        EvEnd,
    };

    struct TEvSecretsResolved: public TEventLocal<TEvSecretsResolved, EvSecretsResolved> {
        TEvSecretsResolved(ui32 channelIdx, TEvDescribeSecretsResponse::TDescription description)
            : ChannelIdx(channelIdx)
            , Description(std::move(description))
        {
        }

        const ui32 ChannelIdx;
        const TEvDescribeSecretsResponse::TDescription Description;
    };

    struct TEvPoll: public TEventLocal<TEvPoll, EvPoll> {};
};

// Everything the DDL has to learn about a single S3-backed channel before the depot can be allocated.
struct TChannelState {
    NKqpProto::TKqpKeyValueVolumeS3Channel Desc;
    NKikimrSchemeOp::TAuth Auth;
    NKikimrBlobDepot::TS3BackendSettings S3Settings;
    TString VirtualPoolName;    // BSC name of the pool the virtual group itself lives in
    TString DepotPoolName;      // BSC name of the pool holding the depot tablet's own channels
    std::optional<ui32> GroupId;
    bool NeedsAllocation = false;
};

class TAllocateS3ChannelsActor: public TActorBootstrapped<TAllocateS3ChannelsActor> {
    enum class EBscPhase {
        Query,      // read the storage pools and look for already allocated depots
        Allocate,   // create the missing depots
        Poll,       // wait until every depot is WORKING
    };

public:
    TAllocateS3ChannelsActor(const NKqpProto::TKqpCreateKeyValueVolume& operation, const TExternalContext& context,
                             NThreading::TPromise<TYqlConclusionStatus> promise)
        : Database(context.GetDatabase())
        , UserToken(context.GetUserToken() ? new NACLib::TUserToken(*context.GetUserToken()) : nullptr)
        , Promise(std::move(promise))
    {
        Channels.reserve(operation.S3ChannelsSize());
        for (const auto& channel : operation.GetS3Channels()) {
            Channels.emplace_back().Desc = channel;
        }
    }

    void Bootstrap() {
        Deadline = TActivationContext::Now() + AllocationTimeout;

        // One scheme cache request covers the database (for the pool kind -> pool name mapping) and every referenced
        // external data source.
        auto request = std::make_unique<NSchemeCache::TSchemeCacheNavigate>();
        request->DatabaseName = Database;
        if (UserToken) {
            request->UserToken = UserToken;
        }
        {
            auto& entry = request->ResultSet.emplace_back();
            entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
            entry.Path = SplitPath(Database);
        }
        for (const auto& channel : Channels) {
            auto& entry = request->ResultSet.emplace_back();
            entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
            entry.Path = SplitPath(channel.Desc.GetDataSourcePath());
        }

        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.release()));
        Become(&TAllocateS3ChannelsActor::StateNavigate);
    }

    STFUNC(StateNavigate) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            default:
                HandleUnexpected(ev->GetTypeRewrite());
        }
    }

    STFUNC(StateSecrets) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPrivate::TEvSecretsResolved, Handle);
            default:
                HandleUnexpected(ev->GetTypeRewrite());
        }
    }

    STFUNC(StateBsc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvBlobStorage::TEvControllerConfigResponse, Handle);
            hFunc(TEvPrivate::TEvPoll, Handle);
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            default:
                HandleUnexpected(ev->GetTypeRewrite());
        }
    }

private:
    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto& resultSet = ev->Get()->Request->ResultSet;
        if (resultSet.size() != Channels.size() + 1) {
            return Fail(NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR, "Internal error. Unexpected scheme cache reply size");
        }

        if (auto status = ResolveStoragePools(resultSet.front()); status.IsFail()) {
            return Fail(status);
        }

        for (size_t i = 0; i < Channels.size(); ++i) {
            if (auto status = ResolveDataSource(Channels[i], resultSet[i + 1]); status.IsFail()) {
                return Fail(status);
            }
        }

        RequestSecrets();
    }

    TYqlConclusionStatus ResolveStoragePools(const NSchemeCache::TSchemeCacheNavigate::TEntry& entry) {
        if (entry.Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
            return TYqlConclusionStatus::Fail(NYql::TIssuesIds::KIKIMR_SCHEME_ERROR,
                TStringBuilder() << "Failed to resolve database " << Database << ": " << entry.Status);
        }
        if (!entry.DomainDescription) {
            return TYqlConclusionStatus::Fail(NYql::TIssuesIds::KIKIMR_SCHEME_ERROR,
                TStringBuilder() << "Database " << Database << " has no storage pools");
        }

        THashMap<TString, TString> poolByKind;
        for (const auto& pool : entry.DomainDescription->Description.GetStoragePools()) {
            poolByKind[pool.GetKind()] = pool.GetName();
        }

        const auto resolve = [&](const TString& kind, TString& name, TStringBuf setting, ui32 channelIndex) {
            const auto it = poolByKind.find(kind);
            if (it == poolByKind.end()) {
                return TYqlConclusionStatus::Fail(NYql::TIssuesIds::KIKIMR_BAD_REQUEST,
                    TStringBuilder() << setting << " of channel " << channelIndex << " refers to an unknown storage pool kind "
                                     << kind << " in database " << Database);
            }
            name = it->second;
            return TYqlConclusionStatus::Success();
        };

        for (auto& channel : Channels) {
            const ui32 index = channel.Desc.GetChannelIndex();
            if (auto status = resolve(channel.Desc.GetStoragePoolKind(), channel.VirtualPoolName, "STORAGE_POOL", index); status.IsFail()) {
                return status;
            }
            if (auto status = resolve(channel.Desc.GetBlobDepotMedia(), channel.DepotPoolName, "BLOB_DEPOT_MEDIA", index); status.IsFail()) {
                return status;
            }
        }
        return TYqlConclusionStatus::Success();
    }

    TYqlConclusionStatus ResolveDataSource(TChannelState& channel, const NSchemeCache::TSchemeCacheNavigate::TEntry& entry) {
        const TString& path = channel.Desc.GetDataSourcePath();
        if (entry.Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
            return TYqlConclusionStatus::Fail(NYql::TIssuesIds::KIKIMR_SCHEME_ERROR,
                TStringBuilder() << "Failed to resolve external data source " << path << ": " << entry.Status);
        }
        if (entry.Kind != NSchemeCache::TSchemeCacheNavigate::EKind::KindExternalDataSource || !entry.ExternalDataSourceInfo) {
            return TYqlConclusionStatus::Fail(NYql::TIssuesIds::KIKIMR_BAD_REQUEST,
                TStringBuilder() << "DATA_SOURCE " << path << " is not an external data source");
        }

        const auto& description = entry.ExternalDataSourceInfo->Description;
        if (description.GetSourceType() != ObjectStorageSourceType) {
            return TYqlConclusionStatus::Fail(NYql::TIssuesIds::KIKIMR_BAD_REQUEST,
                TStringBuilder() << "DATA_SOURCE " << path << " has SOURCE_TYPE=" << description.GetSourceType()
                                 << ", but a volume channel requires " << ObjectStorageSourceType);
        }

        const auto uri = NColumnShard::NTiers::TS3Uri::ParseUri(description.GetLocation());
        if (uri.IsFail()) {
            return TYqlConclusionStatus::Fail(NYql::TIssuesIds::KIKIMR_BAD_REQUEST,
                TStringBuilder() << "Cannot use LOCATION of " << path << ": " << uri.GetErrorMessage());
        }

        auto& settings = *channel.S3Settings.MutableSettings();
        uri->FillSettings(settings);
        settings.SetObjectKeyPattern(channel.Desc.GetObjectPrefix());
        if (channel.Desc.GetAsyncMode()) {
            channel.S3Settings.MutableAsyncMode();
        } else {
            channel.S3Settings.MutableSyncMode();
        }

        channel.Auth = description.GetAuth();
        if (channel.Auth.identity_case() == NKikimrSchemeOp::TAuth::kAws) {
            settings.SetRegion(channel.Auth.GetAws().GetAwsRegion());
        }
        return TYqlConclusionStatus::Success();
    }

    void RequestSecrets() {
        Become(&TAllocateS3ChannelsActor::StateSecrets);
        SecretsPending = Channels.size();

        auto* actorSystem = TActivationContext::ActorSystem();
        const auto selfId = SelfId();
        for (size_t i = 0; i < Channels.size(); ++i) {
            DescribeExternalDataSourceSecrets(Channels[i].Auth, UserToken, Database, actorSystem)
                .Subscribe([actorSystem, selfId, i](const NThreading::TFuture<TEvDescribeSecretsResponse::TDescription>& f) {
                    actorSystem->Send(selfId, new TEvPrivate::TEvSecretsResolved(i, f.GetValue()));
                });
        }
    }

    void Handle(TEvPrivate::TEvSecretsResolved::TPtr& ev) {
        const auto* msg = ev->Get();
        auto& channel = Channels[msg->ChannelIdx];

        if (msg->Description.Status != Ydb::StatusIds::SUCCESS) {
            if (!FirstSecretsError) {
                FirstSecretsError = TYqlConclusionStatus::Fail(NYql::YqlStatusFromYdbStatus(msg->Description.Status),
                    TStringBuilder() << "Cannot resolve secrets of " << channel.Desc.GetDataSourcePath() << ": "
                                     << msg->Description.Issues.ToString());
            }
        } else if (const auto& secrets = msg->Description.SecretValues; secrets.size() == 2) {
            auto& settings = *channel.S3Settings.MutableSettings();
            settings.SetAccessKey(secrets[0]);
            settings.SetSecretKey(secrets[1]);
        } else if (!FirstSecretsError) {
            FirstSecretsError = TYqlConclusionStatus::Fail(NYql::TIssuesIds::KIKIMR_BAD_REQUEST,
                TStringBuilder() << "DATA_SOURCE " << channel.Desc.GetDataSourcePath()
                                 << " must use AUTH_METHOD=AWS to back a volume channel");
        }

        if (--SecretsPending) {
            return;
        }
        if (FirstSecretsError) {
            return Fail(*FirstSecretsError);
        }
        StartBsc();
    }

    void StartBsc() {
        Become(&TAllocateS3ChannelsActor::StateBsc);
        BscPipe = Register(NTabletPipe::CreateClient(SelfId(), MakeBSControllerID()));

        NKikimrBlobStorage::TConfigRequest request;
        request.AddCommand()->MutableReadStoragePool()->SetBoxId(Max<ui64>());
        auto& query = *request.AddCommand()->MutableQueryBaseConfig();
        query.SetSuppressPDisks(true);
        query.SetSuppressVSlots(true);
        query.SetSuppressNodes(true);
        SendToBsc(request, EBscPhase::Query);
    }

    void SendToBsc(const NKikimrBlobStorage::TConfigRequest& request, EBscPhase phase) {
        Phase = phase;
        auto ev = std::make_unique<TEvBlobStorage::TEvControllerConfigRequest>();
        ev->Record.MutableRequest()->CopyFrom(request);
        NTabletPipe::SendData(SelfId(), BscPipe, ev.release());
    }

    void Handle(TEvBlobStorage::TEvControllerConfigResponse::TPtr& ev) {
        const auto& response = ev->Get()->Record.GetResponse();
        if (!response.GetSuccess()) {
            return Fail(NYql::TIssuesIds::KIKIMR_SCHEME_ERROR,
                TStringBuilder() << "BlobStorage controller request failed: " << response.GetErrorDescription());
        }

        switch (Phase) {
            case EBscPhase::Query:
                return OnQueryResponse(response);
            case EBscPhase::Allocate:
                return OnAllocateResponse(response);
            case EBscPhase::Poll:
                return OnPollResponse(response);
        }
    }

    void OnQueryResponse(const NKikimrBlobStorage::TConfigResponse& response) {
        if (response.StatusSize() != 2) {
            return Fail(NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR, "Internal error. Unexpected BlobStorage controller reply");
        }

        // (BoxId, StoragePoolId) of every pool, by name.
        THashMap<TString, std::pair<ui64, ui64>> poolIds;
        for (const auto& pool : response.GetStatus(0).GetStoragePool()) {
            poolIds[pool.GetName()] = {pool.GetBoxId(), pool.GetStoragePoolId()};
        }

        NKikimrBlobStorage::TConfigRequest request;
        for (auto& channel : Channels) {
            const auto poolIt = poolIds.find(channel.VirtualPoolName);
            if (poolIt == poolIds.end()) {
                return Fail(NYql::TIssuesIds::KIKIMR_BAD_REQUEST,
                    TStringBuilder() << "Storage pool " << channel.VirtualPoolName << " of channel "
                                     << channel.Desc.GetChannelIndex() << " is not registered in the BlobStorage controller");
            }
            const auto [boxId, poolId] = poolIt->second;

            // A channel resolves to a pool, not to a group, so the pool must hold exactly one group - ours.
            for (const auto& group : response.GetStatus(1).GetBaseConfig().GetGroup()) {
                if (group.GetBoxId() != boxId || group.GetStoragePoolId() != poolId) {
                    continue;
                }
                if (group.GetVirtualGroupInfo().GetName() == channel.Desc.GetVirtualGroupName()) {
                    channel.GroupId = group.GetGroupId();
                    continue;
                }
                return Fail(NYql::TIssuesIds::KIKIMR_BAD_REQUEST,
                    TStringBuilder() << "Storage pool " << channel.VirtualPoolName << " of channel "
                                     << channel.Desc.GetChannelIndex() << " already contains group " << group.GetGroupId()
                                     << "; an S3-backed channel needs a dedicated storage pool");
            }

            if (channel.GroupId) {
                continue;
            }
            channel.NeedsAllocation = true;
            FillAllocateCommand(*request.AddCommand()->MutableAllocateVirtualGroup(), channel);
        }

        if (request.CommandSize()) {
            return SendToBsc(request, EBscPhase::Allocate);
        }
        Poll();
    }

    void FillAllocateCommand(NKikimrBlobStorage::TAllocateVirtualGroup& cmd, const TChannelState& channel) const {
        cmd.SetName(channel.Desc.GetVirtualGroupName());
        cmd.SetDatabase(Database);
        cmd.SetStoragePoolName(channel.VirtualPoolName);
        cmd.MutableS3BackendSettings()->CopyFrom(channel.S3Settings);

        // The depot keeps its own log and snapshot on system channels and falls back to local data channels only in
        // the asynchronous mode; the layout matches what `dstool group virtual create` produces.
        for (int i = 0; i < 2; ++i) {
            auto& profile = *cmd.AddChannelProfiles();
            profile.SetStoragePoolName(channel.DepotPoolName);
            profile.SetChannelKind(NKikimrBlobDepot::TChannelKind::System);
            profile.SetCount(1);
        }
        auto& data = *cmd.AddChannelProfiles();
        data.SetStoragePoolName(channel.DepotPoolName);
        data.SetChannelKind(NKikimrBlobDepot::TChannelKind::Data);
        data.SetCount(1);
    }

    void OnAllocateResponse(const NKikimrBlobStorage::TConfigResponse& response) {
        size_t statusIdx = 0;
        for (auto& channel : Channels) {
            if (!channel.NeedsAllocation) {
                continue;
            }
            if (statusIdx >= response.StatusSize() || !response.GetStatus(statusIdx).GroupIdSize()) {
                return Fail(NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR,
                    TStringBuilder() << "Internal error. BlobStorage controller returned no group for channel "
                                     << channel.Desc.GetChannelIndex());
            }
            channel.GroupId = response.GetStatus(statusIdx).GetGroupId(0);
            ++statusIdx;
        }
        Poll();
    }

    void Poll() {
        NKikimrBlobStorage::TConfigRequest request;
        auto& query = *request.AddCommand()->MutableQueryBaseConfig();
        query.SetVirtualGroupsOnly(true);
        query.SetSuppressPDisks(true);
        query.SetSuppressVSlots(true);
        query.SetSuppressNodes(true);
        SendToBsc(request, EBscPhase::Poll);
    }

    void OnPollResponse(const NKikimrBlobStorage::TConfigResponse& response) {
        if (!response.StatusSize()) {
            return Fail(NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR, "Internal error. Unexpected BlobStorage controller reply");
        }

        THashMap<TString, const NKikimrBlobStorage::TBaseConfig::TVirtualGroupInfo*> byName;
        for (const auto& group : response.GetStatus(0).GetBaseConfig().GetGroup()) {
            byName[group.GetVirtualGroupInfo().GetName()] = &group.GetVirtualGroupInfo();
        }

        bool ready = true;
        for (const auto& channel : Channels) {
            const auto it = byName.find(channel.Desc.GetVirtualGroupName());
            if (it == byName.end()) {
                ready = false;
                continue;
            }
            switch (it->second->GetState()) {
                case NKikimrBlobStorage::EVirtualGroupState::WORKING:
                    break;
                case NKikimrBlobStorage::EVirtualGroupState::CREATE_FAILED:
                    return Fail(NYql::TIssuesIds::KIKIMR_SCHEME_ERROR,
                        TStringBuilder() << "BlobDepot for channel " << channel.Desc.GetChannelIndex()
                                         << " could not be created: " << it->second->GetErrorReason());
                default:
                    ready = false;
                    break;
            }
        }

        if (ready) {
            return Reply(TYqlConclusionStatus::Success());
        }
        if (TActivationContext::Now() >= Deadline) {
            return Fail(NYql::TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE,
                "Timed out waiting for the BlobDepot of an S3-backed volume channel to start");
        }
        Schedule(PollPeriod, new TEvPrivate::TEvPoll());
    }

    void Handle(TEvPrivate::TEvPoll::TPtr&) {
        Poll();
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        if (ev->Get()->Status != NKikimrProto::OK) {
            Fail(NYql::TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE,
                TStringBuilder() << "Cannot reach the BlobStorage controller, status: " << NKikimrProto::EReplyStatus_Name(ev->Get()->Status));
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr&) {
        Fail(NYql::TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE, "Lost connection to the BlobStorage controller");
    }

    void HandleUnexpected(ui32 eventType) {
        Fail(NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR,
            TStringBuilder() << "Internal error. Unexpected event while allocating volume S3 channels: " << eventType);
    }

    void Fail(NYql::EYqlIssueCode code, const TString& message) {
        Reply(TYqlConclusionStatus::Fail(code, message));
    }

    void Fail(const TYqlConclusionStatus& status) {
        Reply(status);
    }

    void Reply(const TYqlConclusionStatus& status) {
        if (!Promise.HasValue()) {
            Promise.SetValue(status);
        }
        PassAway();
    }

    void PassAway() override {
        if (BscPipe) {
            NTabletPipe::CloseClient(SelfId(), BscPipe);
        }
        if (!Promise.HasValue()) {
            Promise.SetValue(TYqlConclusionStatus::Fail(NYql::TIssuesIds::KIKIMR_OPERATION_ABORTED, "Shutting down"));
        }
        TActorBootstrapped::PassAway();
    }

private:
    const TString Database;
    const TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    NThreading::TPromise<TYqlConclusionStatus> Promise;
    std::vector<TChannelState> Channels;

    size_t SecretsPending = 0;
    std::optional<TYqlConclusionStatus> FirstSecretsError;

    TActorId BscPipe;
    EBscPhase Phase = EBscPhase::Query;
    TInstant Deadline;
};

}   // anonymous namespace

TAsyncStatus AllocateS3Channels(const NKqpProto::TKqpCreateKeyValueVolume& operation, const TExternalContext& context) {
    auto* actorSystem = context.GetActorSystem();
    if (!actorSystem) {
        return NThreading::MakeFuture(TYqlConclusionStatus::Fail(NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR,
            "Internal error. An S3-backed volume channel needs an actor system. Please contact internal support"));
    }

    auto promise = NThreading::NewPromise<TYqlConclusionStatus>();
    actorSystem->Register(new TAllocateS3ChannelsActor(operation, context, promise));
    return promise.GetFuture();
}

}   // namespace NKikimr::NKqp
