#include "get_queue_attributes.h"
#include "actor.h"
#include "config.h"
#include "error.h"
#include "request.h"
#include "utils.h"

#include <ydb/core/http_proxy/events.h>
#include <ydb/core/protos/grpc_pq_old.pb.h>
#include <ydb/core/ymq/base/limits.h>
#include <ydb/core/ymq/error/error.h>
#include <ydb/services/sqs_topic/queue_url/arn.h>
#include <ydb/services/sqs_topic/queue_url/consumer.h>
#include <ydb/services/sqs_topic/queue_url/utils.h>
#include <ydb/services/sqs_topic/queue_url/holder/queue_url_holder.h>

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

#include <library/cpp/json/json_writer.h>

#include <concepts>

using namespace NActors;
using namespace NKikimrClient;

namespace NKikimr::NSqsTopic::V1 {
    using namespace NGRpcService;
    using namespace NGRpcProxy::V1;

    struct TAttributeInfo {
        bool NeedRuntimeAttributes = false;
        bool NeedConsumer = false; // attribute not inherited from the topic
        bool FifoOnly = false;
        bool DlqOnly = false;
    };

    static const TMap<TString, TAttributeInfo> ATTRIBUTES_INFO = {
        {"ApproximateNumberOfMessages", {true, false, false, false}},
        {"ApproximateNumberOfMessagesDelayed", {true, true, false, false}},
        {"ApproximateNumberOfMessagesNotVisible", {true, true, false, false}},
        {"CreatedTimestamp", {true, false, false, false}},
        {"LastModifiedTimestamp", {true, false, false, false}},
        {"DelaySeconds", {false, false, false, false}},
        {"MaximumMessageSize", {false, false, false, false}},
        {"MessageRetentionPeriod", {false, false, false, false}},
        {"ReceiveMessageWaitTimeSeconds", {false, false, false, false}},
        {"RedrivePolicy", {false, true, false, true}},
        {"VisibilityTimeout", {false, false, false, false}},
        {"FifoQueue", {false, true, true, false}},
        {"ContentBasedDeduplication", {false, true, true, false}},
        {"QueueArn", {false, false, false, false}},
    };

    struct TAttributesRequest {
        TSet<TString> Attributes;
        bool NeedRuntimeAttributes = false;
    };

    template <class TProtoRequest>
    static std::expected<TRichQueueUrl, TString> ParseQueueUrlFromRequest(NKikimr::NGRpcService::IRequestOpCtx* request) {
        return ParseQueueUrl(GetRequest<TProtoRequest>(request).queue_url());
    }

    class TGetQueueAttributesActor: public TQueueUrlHolder, public TGrpcActorBase<TGetQueueAttributesActor, TEvSqsTopicGetQueueAttributesRequest> {
    protected:
        using TBase = TGrpcActorBase<TGetQueueAttributesActor, TEvSqsTopicGetQueueAttributesRequest>;
        using TProtoRequest = typename TBase::TProtoRequest;

        static const inline TString Method = "GetQueueAttributes";
    public:
        TGetQueueAttributesActor(NKikimr::NGRpcService::IRequestOpCtx* request)
            : TQueueUrlHolder(ParseQueueUrlFromRequest<TProtoRequest>(request))
            , TBase(request, TQueueUrlHolder::GetTopicPath().value_or(""))
        {
        }

        ~TGetQueueAttributesActor() = default;

        void Bootstrap(const NActors::TActorContext& ctx) {
            TBase::Bootstrap(ctx);

            const Ydb::Ymq::V1::GetQueueAttributesRequest& request = Request();
            if (request.queue_url().empty()) {
                return ReplyWithError(MakeError(NSQS::NErrors::MISSING_PARAMETER, "No QueueUrl parameter."));
            }
            if (!FormalValidQueueUrl()) {
                return ReplyWithError(MakeError(NSQS::NErrors::INVALID_PARAMETER_VALUE, "Invalid QueueUrl"));
            }

            SendDescribeProposeRequest(ctx);
            Become(&TGetQueueAttributesActor::StateWork);
        }


        std::expected<TAttributesRequest, TString> MakeAttributesList() const {
            const Ydb::Ymq::V1::GetQueueAttributesRequest& request = Request();
            TAttributesRequest req;
            auto addAttribute = [&req](const TString& name, const TAttributeInfo& info) {
                req.Attributes.insert(name);
                req.NeedRuntimeAttributes |= info.NeedRuntimeAttributes;
            };
            bool all = false;
            for (const auto& attrName : request.attribute_names()) {
                if (attrName == "All") {
                    all = true;
                    continue;
                }
                const auto* info = ATTRIBUTES_INFO.FindPtr(attrName);
                if (!info) {
                    return std::unexpected(std::format(R"-(Invalid attribute name "{}")-", attrName.ConstRef()));
                }
                addAttribute(attrName, *info);
            }
            if (all) {
                for (const auto& [attrName, info] : ATTRIBUTES_INFO) {
                    if (info.NeedConsumer && !ConsumerConfig) {
                        continue;
                    }
                    if (info.DlqOnly && !HasDlq()) {
                        continue;
                    }
                    addAttribute(attrName, info);
                }
            }
            return req;
        };

        void StateWork(TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleCacheNavigateResponse);
                default:
                    TBase::StateWork(ev);
            }
        }

        void HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
            const NSchemeCache::TSchemeCacheNavigate* result = ev->Get()->Request.Get();
            Y_ABORT_UNLESS(result->ResultSet.size() == 1);
            const auto& response = result->ResultSet.front();
            if (response.Kind != NSchemeCache::TSchemeCacheNavigate::KindTopic) {
                return ReplyWithError(MakeError(NSQS::NErrors::NON_EXISTENT_QUEUE,
                                                std::format("The specified queue doesn't exist")));
            }

            Y_ABORT_UNLESS(response.PQGroupInfo);
            PQGroup = response.PQGroupInfo->Description;
            SelfInfo = response.Self->Info;
            ConsumerConfig = GetConsumerConfig(PQGroup.GetPQTabletConfig(), QueueUrl_->Consumer);
            if (!ConsumerConfig && QueueUrl_->Consumer != GetDefaultSqsConsumerName()) {
                return ReplyWithError(MakeError(NKikimr::NSQS::NErrors::NON_EXISTENT_QUEUE, std::format("The specified queue doesn't exist (consumer: \"{}\")", QueueUrl_->Consumer.c_str())));
            }

            if (auto attrReq = MakeAttributesList(); attrReq.has_value()) {
                AttributesRequest = std::move(attrReq).value();
            } else {
                return ReplyWithError(MakeError(NSQS::NErrors::INVALID_PARAMETER_VALUE, attrReq.error()));
            }

            ReplyAndDie(ActorContext());
        }

        bool HasDlq() const {
            return ConsumerConfig && ConsumerConfig->GetDeadLetterPolicyEnabled() && ConsumerConfig->GetDeadLetterPolicy() == NKikimrPQ::TPQTabletConfig::DEAD_LETTER_POLICY_MOVE;
        }

        static void AddAttribute(Ydb::Ymq::V1::GetQueueAttributesResult& result, const std::string_view name, const std::string_view value) {
            auto [it, ins] = result.mutable_attributes()->try_emplace(name, value);
            Y_DEBUG_ABORT_UNLESS(ins, "Duplicate attribute name \"%s\"", it->first.c_str());
        }

        static void AddAttribute(Ydb::Ymq::V1::GetQueueAttributesResult& result, const std::string_view name, const bool value) {
            return AddAttribute(result, name, value ? "true"sv : "false"sv);
        }

        static void AddAttribute(Ydb::Ymq::V1::GetQueueAttributesResult& result, const std::string_view name, const std::integral auto value) {
            return AddAttribute(result, name, TStringBuf(ToString(value)));
        }

        bool HasAttribute(const TStringBuf name) const {
            return AttributesRequest.Attributes.contains(name);
        }

        Ydb::Ymq::V1::GetQueueAttributesResult FillAttributes() const {
            const auto& pqTabletConfig = PQGroup.GetPQTabletConfig();
            bool fifo = ConsumerConfig ? ConsumerConfig->GetKeepMessageOrder() : QueueUrl_->Fifo;
            Ydb::Ymq::V1::GetQueueAttributesResult result;

            if (const auto attrName = "ApproximateNumberOfMessages"sv; HasAttribute(attrName)) {
                AddAttribute(result, attrName, 0);
            }
            if (const auto attrName = "ApproximateNumberOfMessagesDelayed"sv; HasAttribute(attrName)) {
                AddAttribute(result, attrName, 0);
            }
            if (const auto attrName = "ApproximateNumberOfMessagesNotVisible"sv; HasAttribute(attrName)) {
                AddAttribute(result, attrName, 0);
            }
            if (const auto attrName = "CreatedTimestamp"sv; HasAttribute(attrName)) {
                TInstant ts = TInstant::MilliSeconds(SelfInfo.GetCreateStep());
                AddAttribute(result, attrName, ts.Seconds());
            }
            if (const auto attrName = "LastModifiedTimestamp"sv; HasAttribute(attrName)) {
                TInstant ts = TInstant::MilliSeconds(SelfInfo.GetCreateStep()); // report create time
                AddAttribute(result, attrName, ts.Seconds());
            }
            if (const auto attrName = "DelaySeconds"sv; HasAttribute(attrName)) {
                TDuration delay = TDuration::Seconds(NSQS::TLimits::DelaySeconds);
                if (ConsumerConfig) {
                    delay = TDuration::MilliSeconds(ConsumerConfig->GetDefaultDelayMessageTimeMs());
                }
                AddAttribute(result, attrName, delay.Seconds());
            }
            if (const auto attrName = "MaximumMessageSize"sv; HasAttribute(attrName)) {
                AddAttribute(result, attrName, NSQS::TLimits::MaxMessageSize);
            }
            if (const auto attrName = "MessageRetentionPeriod"sv; HasAttribute(attrName)) {
                TDuration topicRetentionPeriod = TDuration::Seconds(pqTabletConfig.GetPartitionConfig().GetLifetimeSeconds());
                TDuration availabilityPeriod = ConsumerConfig ? TDuration::MilliSeconds(ConsumerConfig->GetAvailabilityPeriodMs()) : TDuration::Zero();
                TDuration duration = Max(topicRetentionPeriod, availabilityPeriod);
                AddAttribute(result, attrName, duration.Seconds());
            }
            if (const auto attrName = "ReceiveMessageWaitTimeSeconds"sv; HasAttribute(attrName)) {
                TDuration waitTime = TDuration::Zero();
                if (ConsumerConfig) {
                    waitTime = TDuration::MilliSeconds(ConsumerConfig->GetDefaultReceiveMessageWaitTimeMs());
                }
                AddAttribute(result, attrName, waitTime.Seconds());
            }
            if (const auto attrName = "VisibilityTimeout"sv; HasAttribute(attrName)) {
                TDuration timeout = TDuration::Seconds(NSQS::TLimits::VisibilityTimeout);
                if (ConsumerConfig) {
                    timeout = TDuration::Seconds(ConsumerConfig->GetDefaultProcessingTimeoutSeconds());
                }
                AddAttribute(result, attrName, timeout.Seconds());
            }
            if (const auto attrName = "FifoQueue"sv; HasAttribute(attrName)) {
                AddAttribute(result, attrName, fifo);
            }
            if (const auto attrName = "ContentBasedDeduplication"sv; HasAttribute(attrName)) {
                bool value = fifo && ConsumerConfig && ConsumerConfig->GetContentBasedDeduplication();
                AddAttribute(result, attrName, value);
            }
            if (const auto attrName = "RedrivePolicy"sv; HasAttribute(attrName)) {
                NJson::TJsonValue redrivePolicy;
                if (HasDlq()) {
                    redrivePolicy["maxReceiveCount"] = ConsumerConfig->GetMaxProcessingAttempts();
                    if (const TString& dlq = ConsumerConfig->GetDeadLetterQueue(); !dlq.empty()) {
                        TRichQueueUrl dlqUrl{
                            .Database = QueueUrl_->Database,
                            .TopicPath = dlq,
                            .Consumer = QueueUrl_->Consumer,
                            .Fifo = QueueUrl_->Fifo,
                        };
                        redrivePolicy["deadLetterTargetArn"] = MakeQueueArn(Cfg().GetYandexCloudMode(), Cfg().GetYandexCloudServiceRegion(), "", dlqUrl);
                    }
                }
                TString json = WriteJson(redrivePolicy, false);
                AddAttribute(result, attrName, json);
            }
            if (const auto attrName = "QueueArn"sv; HasAttribute(attrName)) {
                AddAttribute(result, attrName, MakeQueueArn(Cfg().GetYandexCloudMode(), Cfg().GetYandexCloudServiceRegion(), "", *QueueUrl_));
            }
            return result;
        }

        void ReplyAndDie(const TActorContext& ctx) {
            Ydb::Ymq::V1::GetQueueAttributesResult result = FillAttributes();
            return ReplyWithResult(Ydb::StatusIds::SUCCESS, result, ctx);
        }

    protected:
        const TProtoRequest& Request() const {
            return GetRequest<TProtoRequest>(this->Request_.get());
        }
    private:
        TAttributesRequest AttributesRequest;
        NKikimrSchemeOp::TDirEntry SelfInfo;
        NKikimrSchemeOp::TPersQueueGroupDescription PQGroup;
        TMaybe<NKikimrPQ::TPQTabletConfig::TConsumer> ConsumerConfig;
    };

    std::unique_ptr<NActors::IActor> CreateGetQueueAttributesActor(NKikimr::NGRpcService::IRequestOpCtx* msg) {
        return std::make_unique<TGetQueueAttributesActor>(msg);
    }
} // namespace NKikimr::NSqsTopic::V1
