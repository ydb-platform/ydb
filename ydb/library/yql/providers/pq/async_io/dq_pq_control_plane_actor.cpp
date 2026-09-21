#include "dq_pq_control_plane_actor.h"

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/yql/providers/pq/common/events.h>
#include <ydb/public/sdk/cpp/src/library/issue/yql_issue_message.h>

#include <util/generic/hash_set.h>
#include <util/string/builder.h>

namespace NYql::NDq {

namespace {

using namespace NActors;

class TDqPqControlPlaneActor final : public TActor<TDqPqControlPlaneActor>, public IActorExceptionHandler {
    using TBase = TActor<TDqPqControlPlaneActor>;
    using TDescribeResult = NPq::NProto::TEvDescribeConsumerResult;

    struct TEvDescribeFinished : TEventLocal<TEvDescribeFinished, TPqControlPlaneEvents::EvEnd> {
        TEvDescribeFinished(TString key, NYdb::NTopic::TAsyncDescribeConsumerResult result)
            : Key(std::move(key))
            , Result(std::move(result))
        {}

        const TString Key;
        const NYdb::NTopic::TAsyncDescribeConsumerResult Result;
    };

    struct TDescription {
        ITopicClient::TPtr Client;
        std::optional<TDescribeResult> Result;
        THashMap<ui64, ui32> PartitionIndexes;
        std::vector<TPqControlPlaneEvents::TEvDescribeConsumer::TPtr> Waiters;
    };

public:
    TDqPqControlPlaneActor(NYdb::TDriver driver, IStructuredTokenCredentialsFactory::TPtr credentialsFactory, IPqStaticGateway::TPtr pqGateway, const THashMap<TString, TString>& secureParams)
        : TBase(&TThis::StateFunc)
        , Driver(std::move(driver))
        , CredentialsFactory(std::move(credentialsFactory))
        , PqGateway(std::move(pqGateway))
        , SecureParams(secureParams)
    {}

    STRICT_STFUNC(StateFunc,
        hFunc(TPqControlPlaneEvents::TEvDescribeConsumer, Handle);
        hFunc(TEvDescribeFinished, Handle);
        hFunc(TEvents::TEvUndelivered, Handle);
        hFunc(TEvInterconnect::TEvNodeDisconnected, Handle);
        cFunc(TEvents::TEvPoison::EventType, PassAway);
        IgnoreFunc(TEvInterconnect::TEvNodeConnected);
    )

    STRICT_STFUNC(StateError,
        hFunc(TPqControlPlaneEvents::TEvDescribeConsumer, ReplyError);
        cFunc(TEvents::TEvPoison::EventType, PassAway);
        IgnoreFunc(TEvDescribeFinished);
        IgnoreFunc(TEvents::TEvUndelivered);
        IgnoreFunc(TEvInterconnect::TEvNodeConnected);
        IgnoreFunc(TEvInterconnect::TEvNodeDisconnected);
    )

private:
    bool OnUnhandledException(const std::exception& ex) final {
        Fail(Ydb::StatusIds::INTERNAL_ERROR, ex.what());
        return true;
    }

    void PassAway() final {
        UnsubscribeFromSessions();
        TBase::PassAway();
    }

    void Handle(TEvents::TEvUndelivered::TPtr& ev) {
        Fail(Ydb::StatusIds::UNAVAILABLE, TStringBuilder()
            << "Failed to deliver PQ control-plane event to " << ev->Sender
            << ", event type: " << ev->Get()->SourceType << ", reason: " << ev->Get()->Reason);
    }

    void Handle(TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
        Fail(Ydb::StatusIds::UNAVAILABLE, TStringBuilder()
            << "PQ control-plane reader node disconnected: " << ev->Get()->NodeId);
    }

    void Handle(TPqControlPlaneEvents::TEvDescribeConsumer::TPtr& ev) {
        CurrentRequest = ev.Release();

        const auto& connection = CurrentRequest->Get()->Record.GetConnection();
        const TString key = connection.SerializeAsString();
        const auto [it, inserted] = Descriptions.try_emplace(key);
        auto& description = it->second;
        if (description.Result) {
            Reply(description, CurrentRequest);
            CurrentRequest.Reset();
            return;
        }

        description.Waiters.emplace_back(CurrentRequest.Release());
        if (!inserted) {
            return;
        }

        auto settings = PqGateway->GetTopicClientSettings();
        settings
            .DiscoveryEndpoint(connection.GetEndpoint())
            .Database(connection.GetDatabase())
            .SslCredentials(NYdb::TSslCredentials(connection.GetUseSsl()))
            .CredentialsProviderFactory(CredentialsFactory->Create(SecureParams.Value(connection.GetTokenName(), TString()), connection.GetAddBearerToToken()));

        description.Client = PqGateway->GetTopicClient(Driver, settings);
        description.Client->DescribeConsumer(
            connection.GetTopicPath(),
            connection.GetConsumerName(),
            NYdb::NTopic::TDescribeConsumerSettings().IncludeStats(true)
        ).Subscribe([key, selfId = SelfId(), actorSystem = TActivationContext::ActorSystem()](const auto& future) {
            actorSystem->Send(selfId, new TEvDescribeFinished(key, future));
        });
    }

    void Handle(TEvDescribeFinished::TPtr& ev) {
        const auto& result = ev->Get()->Result.GetValue();
        const auto status = static_cast<Ydb::StatusIds::StatusCode>(result.GetStatus());

        auto& description = Descriptions.at(ev->Get()->Key);
        auto& response = description.Result.emplace();
        response.SetStatus(Ydb::StatusIds::StatusCode_IsValid(status) ? status : Ydb::StatusIds::EXTERNAL_ERROR);
        NYdb::NIssue::IssuesToMessage(result.GetIssues(), response.MutableIssues());

        if (result.IsSuccess()) {
            for (const auto& partition : result.GetConsumerDescription().GetPartitions()) {
                auto* offsets = response.AddPartitions();
                offsets->SetPartitionId(partition.GetPartitionId());
                if (const auto& stats = partition.GetPartitionStats()) {
                    offsets->SetStartOffset(stats->GetStartOffset());
                }
                if (const auto& stats = partition.GetPartitionConsumerStats()) {
                    offsets->SetCommittedOffset(stats->GetCommittedOffset());
                }
            }
        }

        for (ui32 i = 0; i < description.Result->PartitionsSize(); ++i) {
            description.PartitionIndexes.emplace(description.Result->GetPartitions(i).GetPartitionId(), i);
        }

        for (const auto& waiter : description.Waiters) {
            Reply(description, waiter);
        }

        description.Waiters.clear();
        description.Client.Reset();
    }

    void UnsubscribeFromSessions() {
        for (const auto nodeId : SubscribedNodes) {
            Send(TActivationContext::InterconnectProxy(nodeId), new TEvents::TEvUnsubscribe());
        }
        SubscribedNodes.clear();
    }

    void Reply(const TDescription& description, const TPqControlPlaneEvents::TEvDescribeConsumer::TPtr& request) {
        TDescribeResult record;
        record.SetStatus(description.Result->GetStatus());
        *record.MutableIssues() = description.Result->GetIssues();

        const auto& requestedPartitions = request->Get()->Record.GetPartitionIds();
        record.MutablePartitions()->Reserve(requestedPartitions.size());
        for (const auto partitionId : requestedPartitions) {
            if (const auto it = description.PartitionIndexes.find(partitionId); it != description.PartitionIndexes.end()) {
                *record.AddPartitions() = description.Result->GetPartitions(it->second);
            }
        }

        Reply(std::move(record), request);
    }

    void Reply(TDescribeResult result, const TPqControlPlaneEvents::TEvDescribeConsumer::TPtr& request) {
        auto response = std::make_unique<TPqControlPlaneEvents::TEvDescribeConsumerResult>();
        response->Record = std::move(result);

        TEventFlags flags = IEventHandle::FlagTrackDelivery;
        if (const auto nodeId = request->Sender.NodeId(); nodeId && nodeId != SelfId().NodeId()) {
            SubscribedNodes.emplace(nodeId);
            flags |= IEventHandle::FlagSubscribeOnSession;
        }

        Send(request->Sender, response.release(), flags, request->Cookie);
    }

    void ReplyError(TPqControlPlaneEvents::TEvDescribeConsumer::TPtr& request) {
        Reply(FatalError, request);
    }

    void Fail(const Ydb::StatusIds::StatusCode status, const TString& message) {
        FatalError.SetStatus(status);
        NYdb::NIssue::IssueToMessage(NYdb::NIssue::TIssue(message), FatalError.AddIssues());

        Become(&TThis::StateError);
        UnsubscribeFromSessions();

        if (CurrentRequest) {
            ReplyError(CurrentRequest);
            CurrentRequest.Reset();
        }

        for (auto& [_, description] : Descriptions) {
            for (auto& waiter : description.Waiters) {
                ReplyError(waiter);
            }
        }

        Descriptions.clear();
    }

    const NYdb::TDriver Driver;
    const IStructuredTokenCredentialsFactory::TPtr CredentialsFactory;
    const IPqStaticGateway::TPtr PqGateway;
    const THashMap<TString, TString> SecureParams;
    TPqControlPlaneEvents::TEvDescribeConsumer::TPtr CurrentRequest;
    THashSet<ui32> SubscribedNodes;
    TDescribeResult FatalError;
    THashMap<TString, TDescription> Descriptions;
};

} // anonymous namespace

NActors::IActor* CreateDqPqControlPlaneActor(
    NYdb::TDriver driver,
    IStructuredTokenCredentialsFactory::TPtr credentialsFactory,
    IPqStaticGateway::TPtr pqGateway,
    const THashMap<TString, TString>& secureParams)
{
    return new TDqPqControlPlaneActor(std::move(driver), std::move(credentialsFactory), std::move(pqGateway), secureParams);
}

void RegisterDqPqControlPlaneActorFactory(
    TDqAsyncIoFactory& factory,
    NYdb::TDriver driver,
    IStructuredTokenCredentialsFactory::TPtr credentialsFactory,
    IPqStaticGateway::TPtr pqGateway)
{
    factory.RegisterControlPlane(
        PqControlPlaneActorType,
        [driver = std::move(driver), credentialsFactory = std::move(credentialsFactory), pqGateway = std::move(pqGateway)](IDqAsyncIoFactory::TControlPlaneArguments&& args) {
            return CreateDqPqControlPlaneActor(driver, credentialsFactory, pqGateway, args.SecureParams);
        }
    );
}

} // namespace NYql::NDq
