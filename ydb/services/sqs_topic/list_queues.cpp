#include "list_queues.h"
#include "actor.h"
#include "error.h"
#include "request.h"
#include "utils.h"

#include <ydb/core/http_proxy/events.h>
#include <ydb/core/persqueue/public/list_topics/list_all_topics_actor.h>
#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/protos/grpc_pq_old.pb.h>
#include <ydb/core/ymq/base/limits.h>
#include <ydb/core/ymq/error/error.h>
#include <ydb/services/sqs_topic/queue_url/consumer.h>
#include <ydb/services/sqs_topic/queue_url/utils.h>

#include <ydb/core/grpc_services/service_sqs_topic.h>
#include <ydb/core/grpc_services/grpc_request_proxy.h>
#include <ydb/core/grpc_services/rpc_deferrable.h>
#include <ydb/core/grpc_services/rpc_scheme_base.h>
#include <ydb/core/protos/sqs.pb.h>

#include <ydb/public/api/protos/ydb_topic.pb.h>

#include <ydb/library/http_proxy/error/error.h>

#include <ydb/services/sqs_topic/sqs_topic_proxy.h>

#include <ydb/public/api/grpc/draft/ydb_ymq_v1.pb.h>

#include <ydb/core/client/server/grpc_base.h>
#include <ydb/core/grpc_services/rpc_calls.h>

#include <ydb/library/grpc/server/grpc_server.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <ydb/core/persqueue/public/mlp/mlp.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/services/sqs_topic/statuses.h>

namespace NKikimr::NSqsTopic::V1 {

    using namespace NGRpcService;
    using namespace NGRpcProxy::V1;

    constexpr int MAX_LIST_QUEUES_RESULT = 1'000;

    class TListQueuesActor: public TGrpcActorBase<TListQueuesActor, TEvSqsTopicListQueuesRequest> {
        using TBase = TGrpcActorBase<TListQueuesActor, TEvSqsTopicListQueuesRequest>;
        using TProtoRequest = typename TBase::TProtoRequest;

        static inline const TString Method = "ListQueues";

    public:
        TListQueuesActor(NKikimr::NGRpcService::IRequestOpCtx* request);
        ~TListQueuesActor() = default;

        void Bootstrap(const NActors::TActorContext& ctx);

        void StateWork(TAutoPtr<IEventHandle>& ev);
        void HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev);
        void Handle(TEvPQ::TEvListAllTopicsResponse::TPtr& ev);
        void Handle(NPQ::NDescriber::TEvDescribeTopicsResponse::TPtr& ev, const TActorContext& ctx);

    private:
        const TProtoRequest& Request() const {
            return GetRequest<TProtoRequest>(this->Request_.get());
        }

    private:
        TString DatabaseName_;
        TActorId DescriberActorId;
    };

    TListQueuesActor::TListQueuesActor(NKikimr::NGRpcService::IRequestOpCtx* request)
        : TBase(request, "")
        , DatabaseName_(Request_->GetDatabaseName().GetOrElse(""))
    {
    }

    void TListQueuesActor::Bootstrap(const NActors::TActorContext& ctx) {
        TBase::Bootstrap(ctx);
        const auto& request = Request();
        const int maxResults = request.has_max_results() ? request.max_results() : MAX_LIST_QUEUES_RESULT;
        if (this->Request_->GetSerializedToken().empty()) {
            if (AppData(ctx)->EnforceUserTokenRequirement || AppData(ctx)->PQConfig.GetRequireCredentialsInNewProtocol()) {
                return ReplyWithError(MakeError(NSQS::NErrors::ACCESS_DENIED, "Unauthenticated access is forbidden, please provide credentials"));
            }
        }
        if (DatabaseName_.empty()) {
            return ReplyWithError(MakeError(NSQS::NErrors::INVALID_PARAMETER_VALUE, "Request without database is forbidden"));
        }
        if (maxResults > MAX_LIST_QUEUES_RESULT) {
            return ReplyWithError(MakeError(NSQS::NErrors::INVALID_PARAMETER_VALUE, std::format("MaxResults should be not greater than {}", MAX_LIST_QUEUES_RESULT)));
        }
        if (maxResults < 1) {
            return ReplyWithError(MakeError(NSQS::NErrors::INVALID_PARAMETER_VALUE, std::format("MaxResults should be not less than {}", 1)));
        }

        const TString startFrom{TStringBuf(request.next_token()).Before('@')}; // use only queue name

        ctx.Register(NPQ::MakeListAllTopicsActor(SelfId(), DatabaseName_,
                                                 this->Request_->GetSerializedToken(), true,
                                                 startFrom));
        Become(&TListQueuesActor::StateWork);
    }

    void TListQueuesActor::Handle(TEvPQ::TEvListAllTopicsResponse::TPtr& ev) {
        TVector<TString> topics = std::move(ev->Get()->Topics);
        if (const TString prefix = Request().queue_name_prefix(); !prefix.empty()) {
            std::erase_if(topics, [&prefix](const TString& topic) { return !topic.StartsWith(prefix); });
        }
        NPQ::NDescriber::TDescribeSettings settings = {
            .UserToken = MakeIntrusive<NACLib::TUserToken>(this->Request_->GetSerializedToken()),
            .AccessRights = NACLib::EAccessRights::DescribeSchema,
        };
        std::unordered_set<TString> topicsSet(topics.size());
        for (const auto& topic : topics) {
            TString fullTopicPath = CanonizePath(NKikimr::JoinPath({DatabaseName_, topic}));
            topicsSet.insert(std::move(fullTopicPath));
        }
        DescriberActorId = RegisterWithSameMailbox(NPQ::NDescriber::CreateDescriberActor(SelfId(), *Request_->GetDatabaseName(), std::move(topicsSet), settings));
    }

    struct TTopicConsumerPair {
        TString Topic;
        TString Consumer;
        bool Fifo{};

        friend auto operator<=>(const TTopicConsumerPair& lhs, const TTopicConsumerPair& rhs) {
            return std::tie(lhs.Topic, lhs.Consumer) <=> std::tie(rhs.Topic, rhs.Consumer);
        };
    };

    void TListQueuesActor::Handle(NPQ::NDescriber::TEvDescribeTopicsResponse::TPtr& ev, const TActorContext& ctx) {
        DescriberActorId = {};
        const auto& topicsMap = ev->Get()->Topics;
        TVector<TTopicConsumerPair> tc(Reserve(topicsMap.size()));

        for (const auto& [name, describeResult] : topicsMap) {
            if (describeResult.Status != NPQ::NDescriber::EStatus::SUCCESS) {
                return ReplyWithError(MakeError(NSQS::NErrors::INTERNAL_FAILURE, std::format("Description of topic \"{}\" is unsuccessful", name.ConstRef())));
            }
            const auto& info = describeResult.Info;
            for (const auto& consumer : info->Description.GetPQTabletConfig().GetConsumers()) {
                if (consumer.GetType() != NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP) {
                    continue;
                }
                TTopicConsumerPair tcp{
                    .Topic = name,
                    .Consumer = consumer.GetName(),
                    .Fifo = consumer.GetKeepMessageOrder(),
                };
                tc.push_back(std::move(tcp));
            }
        }
        Sort(tc);
        if (Request().has_next_token()) {
            const TString nextToken = Request().next_token();
            const auto pred = [&nextToken](const auto& item) {
                TString name = TString::Join(item.Topic, '@', item.Consumer);
                return name <= nextToken;
            };
            std::erase_if(tc, pred);
        }
        const int maxResults = Request().has_max_results() ? Request().max_results() : MAX_LIST_QUEUES_RESULT;
        TString newNextToken;
        if (std::cmp_greater(tc.size(), maxResults)) {
            if (Request().has_max_results()) { // Use paging only if the limit has been explicitly specified
                tc.resize(maxResults);
                const auto& threshold = tc.back();
                newNextToken = TString::Join(threshold.Topic, '@', threshold.Consumer);
            }
        }

        Ydb::Ymq::V1::ListQueuesResult result;
        if (!newNextToken.empty()) {
            result.set_next_token(newNextToken);
        }

        for (const auto& topicConsumer : tc) {
            TStringBuf topicPath = TStringBuf{topicConsumer.Topic};
            bool isFull = true;
            isFull &= topicPath.SkipPrefix(DatabaseName_);
            isFull &= topicPath.SkipPrefix("/"sv);
            Y_ASSERT(isFull);
            const TRichQueueUrl queueUrl{
                .Database = DatabaseName_,
                .TopicPath = ToString(topicPath),
                .Consumer = topicConsumer.Consumer,
                .Fifo = topicConsumer.Fifo,
            };
            TString path = PackQueueUrlPath(queueUrl);
            TString url = TStringBuilder() << GetEndpoint(Cfg()) << path;
            result.add_queue_urls(url);
        }
        return this->ReplyWithResult(Ydb::StatusIds::SUCCESS, result, ctx);
    }

    void TListQueuesActor::StateWork(TAutoPtr<IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleCacheNavigateResponse); // override for testing
            hFunc(TEvPQ::TEvListAllTopicsResponse, Handle);
            HFunc(NPQ::NDescriber::TEvDescribeTopicsResponse, Handle);
            default:
                TBase::StateWork(ev);
        }
    }

    void TListQueuesActor::HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        Y_UNUSED(ev);
    }

    std::unique_ptr<NActors::IActor> CreateListQueuesActor(NKikimr::NGRpcService::IRequestOpCtx* msg) {
        return std::make_unique<TListQueuesActor>(msg);
    }
} // namespace NKikimr::NSqsTopic::V1
