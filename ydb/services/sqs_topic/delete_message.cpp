#include "delete_message.h"
#include "actor.h"
#include "config.h"
#include "error.h"
#include "receipt.h"
#include "request.h"
#include "utils.h"

#include <ydb/core/http_proxy/events.h>
#include <ydb/core/protos/grpc_pq_old.pb.h>
#include <ydb/core/ymq/base/limits.h>
#include <ydb/core/ymq/error/error.h>
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
#include <ydb/core/persqueue/public/pq_rl_helpers.h>

#include <ydb/services/sqs_topic/billing.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/services/sqs_topic/statuses.h>

using namespace NActors;
using namespace NKikimrClient;

namespace NKikimr::NSqsTopic::V1 {
    using namespace NGRpcService;
    using namespace NGRpcProxy::V1;

    struct TMessageIdLess {
        bool operator()(const NPQ::NMLP::TMessageId& lhs, const NPQ::NMLP::TMessageId& rhs) const {
            return std::tie(lhs.PartitionId, lhs.Offset) < std::tie(rhs.PartitionId, rhs.Offset);
        }
    };

    template <class TProtoRequest>
    static std::expected<TRichQueueUrl, TString> ParseQueueUrlFromRequest(NKikimr::NGRpcService::IRequestOpCtx* request) {
        return ParseQueueUrl(GetRequest<TProtoRequest>(request).queue_url());
    }

    template <class TDerived, class TServiceRequest>
    class TDeleteMessageActorBase: public TQueueUrlHolder, public TGrpcActorBase<TDeleteMessageActorBase<TDerived, TServiceRequest>, TServiceRequest>, private NPQ::TRlHelpers {
    protected:
        using TBase = TGrpcActorBase<TDeleteMessageActorBase, TServiceRequest>;
        using TProtoRequest = typename TBase::TProtoRequest;
        using EWakeupTag = NPQ::TRlHelpers::EWakeupTag;

    public:
        TDeleteMessageActorBase(NKikimr::NGRpcService::IRequestOpCtx* request)
            : TQueueUrlHolder(ParseQueueUrlFromRequest<TProtoRequest>(request))
            , TBase(request, GetTopicPath().value_or(""))
            , NPQ::TRlHelpers({}, request, NBilling::WRITE_BLOCK_SIZE, false)
        {
        }

        ~TDeleteMessageActorBase() = default;

        void Bootstrap(const NActors::TActorContext& ctx) {
            TBase::Bootstrap(ctx);
            NPQ::TRlHelpers::Bootstrap(this->SelfId(), ctx);

            if (this->Request().queue_url().empty()) {
                return this->ReplyWithError(MakeError(NSQS::NErrors::MISSING_PARAMETER, "No QueueUrl parameter."));
            }
            if (!QueueUrl_.has_value()) {
                return this->ReplyWithError(MakeError(NSQS::NErrors::INVALID_PARAMETER_VALUE, "Invalid QueueUrl"));
            }

            TVector<Ydb::Ymq::V1::DeleteMessageBatchRequestEntry> entries = static_cast<TDerived*>(this)->GetEntries();
            TVector<NPQ::NMLP::TMessageId> requestList;
            THashSet<TString> ids;
            if (std::cmp_less(entries.size(), NSQS::TLimits::MinBatchSize)) {
                return this->ReplyWithError(MakeError(NSQS::NErrors::EMPTY_BATCH_REQUEST));
            }
            if (std::cmp_greater(entries.size(), NSQS::TLimits::MaxBatchSize)) {
                return this->ReplyWithError(MakeError(NSQS::NErrors::TOO_MANY_ENTRIES_IN_BATCH_REQUEST));
            }
            for (const auto& entry : entries) {
                const TString& id = entry.id();
                if (!ids.insert(id).second) {
                    return this->ReplyWithError(MakeError(NSQS::NErrors::BATCH_ENTRY_IDS_NOT_DISTINCT));
                }
                auto receipt = DeserializeReceipt(entry.receipt_handle());
                if (!receipt.has_value()) {
                    Failed_[id] = MakeError(NSQS::NErrors::RECEIPT_HANDLE_IS_INVALID);
                    continue;
                }
                requestList.push_back(receipt.value());
                if (!PositionToIdMap_.try_emplace(receipt.value(), id).second) {
                    return this->ReplyWithError(MakeError(NSQS::NErrors::INVALID_PARAMETER_COMBINATION, "Two or more batch entries in the request have the same receipt."));
                }
            }

            this->Become(&TDeleteMessageActorBase::StateWork);

            if (requestList.empty()) {
                static_cast<TDerived*>(this)->ReplyAndDie(ctx);
                return;
            }

            ctx.Send(NHttpProxy::MakeMetricsServiceID(),
                new NHttpProxy::TEvServerlessProxy::TEvCounter{
                    static_cast<i64>(requestList.size()), true, true,
                    GetRequestMessageCountMetricsLabels(
                        QueueUrl_->Database,
                        FullTopicPath_,
                        QueueUrl_->Consumer,
                        TDerived::Method)
                });

            CommitterSettings_ = NPQ::NMLP::TCommitterSettings{
                .DatabasePath = this->QueueUrl_->Database,
                .TopicName = FullTopicPath_,
                .Consumer = ResolveConsumerNameFromQueueUrl(this->QueueUrl_->Consumer, ctx),
                .Messages = std::move(requestList),
                .UserToken = this->Request_->GetInternalToken(),
            };

            this->SendDescribeProposeRequest(ctx);
        }

        void StateWork(TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleCacheNavigateResponse);
                HFunc(NPQ::NMLP::TEvChangeResponse, Handle);
                HFunc(TEvents::TEvWakeup, HandleWakeupTag);
                default:
                    TBase::StateWork(ev);
            }
        }

        void HandleWakeupTag(TEvents::TEvWakeup::TPtr& ev, const TActorContext& ctx) {
            switch (static_cast<EWakeupTag>(ev->Get()->Tag)) {
                case EWakeupTag::RlAllowed:
                    return static_cast<TDerived*>(this)->ReplyAndDie(ctx);
                case EWakeupTag::RlNoResource:
                    return this->ReplyWithError(MakeError(NSQS::NErrors::THROTTLING_EXCEPTION, "Request was throttled by the rate limiter"));
                default:
                    OnWakeup(static_cast<EWakeupTag>(ev->Get()->Tag));
                    return;
            }
        }

        void Handle(NPQ::NMLP::TEvChangeResponse::TPtr& ev, const TActorContext& ctx) {
            CommiterActorId_ = {};
            const NPQ::NMLP::TEvChangeResponse& response = *ev->Get();
            switch (response.Status) {
                case Ydb::StatusIds::SUCCESS: {
                    break;
                }
                case Ydb::StatusIds::SCHEME_ERROR: {
                    this->ReplyWithError(MakeError(NKikimr::NSQS::NErrors::NON_EXISTENT_QUEUE, std::format("Error reading from topic: {}", response.ErrorDescription.ConstRef())));
                    return;
                }
                default: {
                    this->ReplyWithError(MakeError(NSQS::NErrors::INTERNAL_FAILURE, std::format("Error reading from topic: {}", response.ErrorDescription.ConstRef())));
                    return;
                }
            }

            ssize_t successCount = 0;
            ssize_t failedCount = 0;

            for (const auto& message : response.Messages) {
                const TString* id = PositionToIdMap_.FindPtr(message.MessageId);
                if (!id) {
                    this->ReplyWithError(MakeError(NSQS::NErrors::INTERNAL_FAILURE, std::format("Message id not found")));
                    return;
                }
                if (message.Success) {
                    Success_.insert(*id);
                    ++successCount;
                } else {
                    Failed_[*id] = MakeError(NSQS::NErrors::INVALID_PARAMETER_VALUE, {});
                    ++failedCount;
                }
            }

            ctx.Send(NHttpProxy::MakeMetricsServiceID(),
                new NHttpProxy::TEvServerlessProxy::TEvCounter{
                    successCount, true, true,
                    GetResponseMessageCountMetricsLabels(
                        QueueUrl_->Database,
                        FullTopicPath_,
                        QueueUrl_->Consumer,
                        TDerived::Method,
                        "success")
                });
            ctx.Send(NHttpProxy::MakeMetricsServiceID(),
                new NHttpProxy::TEvServerlessProxy::TEvCounter{
                    failedCount, true, true,
                    GetResponseMessageCountMetricsLabels(
                        QueueUrl_->Database,
                        FullTopicPath_,
                        QueueUrl_->Consumer,
                        TDerived::Method,
                        "failed")
                });

            if (IsQuotaRequired() && successCount > 0) {
                const ui64 ru = NBilling::CalcDeleteRu(static_cast<ui64>(successCount), Fifo_, Dedup_);
                Y_ABORT_UNLESS(MaybeRequestQuota(ru, EWakeupTag::RlAllowed, ctx));
                return;
            }

            static_cast<TDerived*>(this)->ReplyAndDie(ctx);
        }

        void Die(const TActorContext& ctx) override {
            if (CommiterActorId_) {
                ctx.Send(CommiterActorId_, new TEvents::TEvPoison);
            }
            NPQ::TRlHelpers::PassAway(this->SelfId());
            this->TBase::Die(ctx);
        }

        void HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
            // Second navigate: resolve the rate-limiter path from the database
            // serverless attributes, then start the committer.
            if (TBase::IsRlPathNavigateResponse(ev)) {
                if (auto rlContext = this->ExtractRlContext(ev)) {
                    SetRlContext(*rlContext);
                }
                CreateCommitter();
                return;
            }

            const NSchemeCache::TSchemeCacheNavigate* result = ev->Get()->Request.Get();
            Y_ABORT_UNLESS(result->ResultSet.size() == 1);
            const auto& response = result->ResultSet.front();
            if (response.Status == NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
                if (response.Kind != NSchemeCache::TSchemeCacheNavigate::KindTopic) {
                    return this->ReplyWithError(MakeError(NSQS::NErrors::NON_EXISTENT_QUEUE, TStringBuilder() << "Queue name used by another scheme object"));
                }
                // ok
            } else if (response.Status == NSchemeCache::TSchemeCacheNavigate::EStatus::PathErrorUnknown) {
                return this->ReplyWithError(MakeError(NKikimr::NSQS::NErrors::NON_EXISTENT_QUEUE, std::format("The specified queue doesn't exist")));
            } else {
                return this->ReplyWithError(MakeError(NSQS::NErrors::INTERNAL_FAILURE,
                                                TStringBuilder() << "Failed to describe topic: " << response.Status));
            }

            const auto& pqTabletConfig = response.PQGroupInfo->Description.GetPQTabletConfig();
            SetMeteringMode(pqTabletConfig.GetMeteringMode());
            Fifo_ = QueueUrl_->Fifo;
            Dedup_ = Fifo_ && pqTabletConfig.GetContentBasedDeduplication();

            // RU-metered topics need the rate-limiter path, which is not carried
            // by DoLocalRpc requests. Resolve it from the database attributes
            // before committing so the charge in Handle(TEvChangeResponse) can fire.
            if (IsRequestUnitsMeteringMode()) {
                this->SendRlPathNavigate();
                return;
            }
            CreateCommitter();
        }

        void CreateCommitter() {
            std::unique_ptr<IActor> actorPtr{NKikimr::NPQ::NMLP::CreateCommitter(this->SelfId(), std::move(*CommitterSettings_))};
            CommiterActorId_ = this->ActorContext().RegisterWithSameMailbox(actorPtr.release());
        }

    protected:
        const TProtoRequest& Request() const {
            return GetRequest<TProtoRequest>(this->Request_.get());
        }

    protected:
        TActorId CommiterActorId_;
        THashMap<TString, NSQS::TError> Failed_;
        THashSet<TString> Success_;
        TMap<NPQ::NMLP::TMessageId, TString, TMessageIdLess> PositionToIdMap_;
        TMaybe<NPQ::NMLP::TCommitterSettings> CommitterSettings_;
        bool Fifo_ = false;
        bool Dedup_ = false;
    };

    class TDeleteMessageActor: public TDeleteMessageActorBase<TDeleteMessageActor, TEvSqsTopicDeleteMessageRequest> {
    public:
        const static inline TString Method = "DeleteMessage";

    public:
        using TBase = TDeleteMessageActorBase<TDeleteMessageActor, TEvSqsTopicDeleteMessageRequest>;
        using TBase::TBase;

        TVector<Ydb::Ymq::V1::DeleteMessageBatchRequestEntry> GetEntries() const {
            const auto& request = this->Request();
            Ydb::Ymq::V1::DeleteMessageBatchRequestEntry entry;
            entry.set_id("placeholder");
            entry.set_receipt_handle(request.receipt_handle());
            return {std::move(entry)};
        }

        void ReplyAndDie(const TActorContext& ctx) {
            if (size_t n = Success_.size() + Failed_.size(); n != 1) {
                this->ReplyWithError(MakeError(NSQS::NErrors::INTERNAL_FAILURE, std::format("Unexpected number of messages to delete: expected={}, got={}", 1, n)));
                return;
            }
            if (Failed_.size()) {
                this->ReplyWithError(Failed_.begin()->second);
                return;
            }
            Ydb::Ymq::V1::DeleteMessageResult result;
            return this->ReplyWithResult(Ydb::StatusIds::SUCCESS, result, ctx);
        }
    };

    class TDeleteMessageBatchActor: public TDeleteMessageActorBase<TDeleteMessageBatchActor, TEvSqsTopicDeleteMessageBatchRequest> {
    public:
        const static inline TString Method = "DeleteMessageBatch";

    public:
        using TBase = TDeleteMessageActorBase<TDeleteMessageBatchActor, TEvSqsTopicDeleteMessageBatchRequest>;
        using TBase::TBase;

        TVector<Ydb::Ymq::V1::DeleteMessageBatchRequestEntry> GetEntries() const {
            const auto& request = this->Request();
            TVector<Ydb::Ymq::V1::DeleteMessageBatchRequestEntry> result(Reserve(request.entries_size()));
            for (const auto& entry : request.entries()) {
                result.push_back(entry);
            }
            return result;
        }

        void ReplyAndDie(const TActorContext& ctx) {
            Ydb::Ymq::V1::DeleteMessageBatchResult result;
            for (const auto& id : Success_) {
                result.add_successful()->set_id(id);
            }
            for (const auto& [id, error] : Failed_) {
                auto* failed = result.add_failed();
                failed->set_id(id);
                failed->set_code(error.GetErrorCode());
                failed->set_sender_fault(IsSenderFailure(error));
                failed->set_message(error.GetMessage());
            }
            return this->ReplyWithResult(Ydb::StatusIds::SUCCESS, result, ctx);
        }
    };

    std::unique_ptr<NActors::IActor> CreateDeleteMessageActor(NKikimr::NGRpcService::IRequestOpCtx* msg) {
        return std::make_unique<TDeleteMessageActor>(msg);
    }

    std::unique_ptr<NActors::IActor> CreateDeleteMessageBatchActor(NKikimr::NGRpcService::IRequestOpCtx* msg) {
        return std::make_unique<TDeleteMessageBatchActor>(msg);
    }
} // namespace NKikimr::NSqsTopic::V1
