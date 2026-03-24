#include "change_message_visibility.h"
#include "actor.h"
#include "error.h"
#include "limits.h"
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
    class TChangeMessageVisibilityActorBase: public TQueueUrlHolder, public TGrpcActorBase<TChangeMessageVisibilityActorBase<TDerived, TServiceRequest>, TServiceRequest> {
    protected:
        using TBase = TGrpcActorBase<TChangeMessageVisibilityActorBase, TServiceRequest>;
        using TProtoRequest = typename TBase::TProtoRequest;

    public:
        TChangeMessageVisibilityActorBase(NKikimr::NGRpcService::IRequestOpCtx* request)
            : TQueueUrlHolder(ParseQueueUrlFromRequest<TProtoRequest>(request))
            , TBase(request, GetTopicPath().value_or(""))
        {
        }

        ~TChangeMessageVisibilityActorBase() = default;

        void Bootstrap(const NActors::TActorContext& ctx) {
            TBase::Bootstrap(ctx);

            if (this->Request().queue_url().empty()) {
                return this->ReplyWithError(MakeError(NSQS::NErrors::MISSING_PARAMETER, "No QueueUrl parameter."));
            }
            if (!QueueUrl_.has_value()) {
                return this->ReplyWithError(MakeError(NSQS::NErrors::INVALID_PARAMETER_VALUE, "Invalid QueueUrl"));
            }

            TVector<Ydb::Ymq::V1::ChangeMessageVisibilityBatchRequestEntry> entries = static_cast<TDerived*>(this)->GetEntries();
            TVector<std::pair<NPQ::NMLP::TMessageId, TInstant>> requestList;
            THashSet<TString> ids;
            if (std::cmp_less(entries.size(), NSQS::TLimits::MinBatchSize)) {
                return this->ReplyWithError(MakeError(NSQS::NErrors::EMPTY_BATCH_REQUEST));
            }
            if (std::cmp_greater(entries.size(), NSQS::TLimits::MaxBatchSize)) {
                return this->ReplyWithError(MakeError(NSQS::NErrors::TOO_MANY_ENTRIES_IN_BATCH_REQUEST));
            }
            const TInstant now = TInstant::Now();
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
                if (!entry.has_visibility_timeout()) {
                    Failed_[id] = MakeError(NSQS::NErrors::MISSING_PARAMETER,
                                            std::format("VisibilityTimeout must be specified"));
                    continue;
                }
                const auto visibilityTimeout = entry.visibility_timeout();
                if (std::cmp_less(visibilityTimeout, MIN_VISIBILITY_TIMEOUT.Seconds()) || std::cmp_greater(visibilityTimeout, MAX_VISIBILITY_TIMEOUT.Seconds())) {
                    Failed_[id] = MakeError(NSQS::NErrors::INVALID_PARAMETER_VALUE,
                                            std::format("VisibilityTimeout must be between {} and {} seconds",
                                                        MIN_VISIBILITY_TIMEOUT.Seconds(), MAX_VISIBILITY_TIMEOUT.Seconds()));
                    continue;
                }
                requestList.emplace_back(receipt.value(), TDuration::Seconds(visibilityTimeout).ToDeadLine(now));
                if (!PositionToIdMap_.try_emplace(receipt.value(), id).second) {
                    return this->ReplyWithError(MakeError(NSQS::NErrors::INVALID_PARAMETER_COMBINATION, "Two or more batch entries in the request have the same receipt."));
                }
            }

            this->Become(&TChangeMessageVisibilityActorBase::StateWork);

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

            TVector<NPQ::NMLP::TMessageId> messages(Reserve(requestList.size()));
            TVector<TInstant> deadlines(Reserve(requestList.size()));
            for (const auto& [messageId, deadline] : requestList) {
                messages.push_back(messageId);
                deadlines.push_back(deadline);
            }

            NPQ::NMLP::TMessageDeadlineChangerSettings changerSettings{
                .DatabasePath = this->QueueUrl_->Database,
                .TopicName = FullTopicPath_,
                .Consumer = this->QueueUrl_->Consumer,
                .Messages = std::move(messages),
                .Deadlines = std::move(deadlines),
                .UserToken = this->Request_->GetInternalToken(),
            };

            std::unique_ptr<IActor> actorPtr{NKikimr::NPQ::NMLP::CreateMessageDeadlineChanger(this->SelfId(), std::move(changerSettings))};
            DeadlineChangerActorId_ = ctx.RegisterWithSameMailbox(actorPtr.release());
        }

        void StateWork(TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleCacheNavigateResponse); // override for testing
                HFunc(NPQ::NMLP::TEvChangeResponse, Handle);
                default:
                    TBase::StateWork(ev);
            }
        }

        void Handle(NPQ::NMLP::TEvChangeResponse::TPtr& ev, const TActorContext& ctx) {
            DeadlineChangerActorId_ = {};
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

            static_cast<TDerived*>(this)->ReplyAndDie(ctx);
        }

        void Die(const TActorContext& ctx) override {
            if (DeadlineChangerActorId_) {
                ctx.Send(DeadlineChangerActorId_, new TEvents::TEvPoison);
            }
            this->TBase::Die(ctx);
        }

        void HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
            Y_UNUSED(ev);
        }

    protected:
        const TProtoRequest& Request() const {
            return GetRequest<TProtoRequest>(this->Request_.get());
        }

    protected:
        TActorId DeadlineChangerActorId_;
        THashMap<TString, NSQS::TError> Failed_;
        THashSet<TString> Success_;
        TMap<NPQ::NMLP::TMessageId, TString, TMessageIdLess> PositionToIdMap_;
    };

    class TChangeMessageVisibilityActor: public TChangeMessageVisibilityActorBase<TChangeMessageVisibilityActor, TEvSqsTopicChangeMessageVisibilityRequest> {
    public:
        static const inline TString Method = "ChangeMessageVisibility";

    public:
        using TBase = TChangeMessageVisibilityActorBase<TChangeMessageVisibilityActor, TEvSqsTopicChangeMessageVisibilityRequest>;
        using TBase::TBase;

        TVector<Ydb::Ymq::V1::ChangeMessageVisibilityBatchRequestEntry> GetEntries() const {
            const auto& request = this->Request();
            Ydb::Ymq::V1::ChangeMessageVisibilityBatchRequestEntry entry;
            entry.set_id("placeholder");
            entry.set_receipt_handle(request.receipt_handle());
            entry.set_visibility_timeout(request.visibility_timeout());
            return {std::move(entry)};
        }

        void ReplyAndDie(const TActorContext& ctx) {
            if (size_t n = Success_.size() + Failed_.size(); n != 1) {
                this->ReplyWithError(MakeError(NSQS::NErrors::INTERNAL_FAILURE, std::format("Unexpected number of messages to change visibility: expected={}, got={}", 1, n)));
                return;
            }
            if (Failed_.size()) {
                this->ReplyWithError(Failed_.begin()->second);
                return;
            }
            Ydb::Ymq::V1::ChangeMessageVisibilityResult result;
            return this->ReplyWithResult(Ydb::StatusIds::SUCCESS, result, ctx);
        }
    };

    class TChangeMessageVisibilityBatchActor: public TChangeMessageVisibilityActorBase<TChangeMessageVisibilityBatchActor, TEvSqsTopicChangeMessageVisibilityBatchRequest> {
    public:
        static const inline TString Method = "ChangeMessageVisibilityBatch";

    public:
        using TBase = TChangeMessageVisibilityActorBase<TChangeMessageVisibilityBatchActor, TEvSqsTopicChangeMessageVisibilityBatchRequest>;
        using TBase::TBase;

        TVector<Ydb::Ymq::V1::ChangeMessageVisibilityBatchRequestEntry> GetEntries() const {
            const auto& request = this->Request();
            TVector<Ydb::Ymq::V1::ChangeMessageVisibilityBatchRequestEntry> result(Reserve(request.entries_size()));
            for (const auto& entry : request.entries()) {
                result.push_back(entry);
            }
            return result;
        }

        void ReplyAndDie(const TActorContext& ctx) {
            Ydb::Ymq::V1::ChangeMessageVisibilityBatchResult result;
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

    std::unique_ptr<NActors::IActor> CreateChangeMessageVisibilityActor(NKikimr::NGRpcService::IRequestOpCtx* msg) {
        return std::make_unique<TChangeMessageVisibilityActor>(msg);
    }

    std::unique_ptr<NActors::IActor> CreateChangeMessageVisibilityBatchActor(NKikimr::NGRpcService::IRequestOpCtx* msg) {
        return std::make_unique<TChangeMessageVisibilityBatchActor>(msg);
    }
} // namespace NKikimr::NSqsTopic::V1
