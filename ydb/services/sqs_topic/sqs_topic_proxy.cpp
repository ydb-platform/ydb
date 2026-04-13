#include "sqs_topic_proxy.h"
#include "actor.h"
#include "change_message_visibility.h"
#include "create_queue.h"
#include "delete_queue.h"
#include "error.h"
#include "delete_message.h"
#include "get_queue_attributes.h"
#include "get_queue_url.h"
#include "list_queues.h"
#include "purge_queue.h"
#include "request.h"
#include "receive_message.h"
#include "send_message.h"
#include "set_queue_attributes.h"
#include "utils.h"

#include <ydb/services/sqs_topic/queue_url/consumer.h>
#include <ydb/services/sqs_topic/queue_url/utils.h>

#include <ydb/core/grpc_services/service_sqs_topic.h>
#include <ydb/core/grpc_services/grpc_request_proxy.h>
#include <ydb/core/grpc_services/rpc_deferrable.h>
#include <ydb/core/grpc_services/rpc_scheme_base.h>
#include <ydb/core/protos/sqs.pb.h>

#include <ydb/public/api/protos/ydb_topic.pb.h>
#include <ydb/services/datastreams/codes/datastreams_codes.h>

#include <ydb/services/lib/sharding/sharding.h>
#include <ydb/services/persqueue_v1/actors/persqueue_utils.h>

#include <ydb/library/http_proxy/error/error.h>

using namespace NActors;
using namespace NKikimrClient;

namespace NKikimr::NSqsTopic::V1 {

    using namespace NGRpcService;
    using namespace NGRpcProxy::V1;

    template <class TEvRequest>
    class TNotImplementedRequestActor: public TRpcSchemeRequestActor<TNotImplementedRequestActor<TEvRequest>, TEvRequest> {
        using TBase = TRpcSchemeRequestActor<TNotImplementedRequestActor, TEvRequest>;

    public:
        TNotImplementedRequestActor(NKikimr::NGRpcService::IRequestOpCtx* request)
            : TBase(request)
        {
        }
        ~TNotImplementedRequestActor() = default;

        void Bootstrap(const NActors::TActorContext& ctx) {
            TBase::Bootstrap(ctx);
            this->Request_->RaiseIssue(FillIssue("Method is not implemented yet", static_cast<size_t>(NYds::EErrorCodes::ERROR)));
            this->Request_->ReplyWithYdbStatus(Ydb::StatusIds::UNSUPPORTED);
            this->Die(ctx);
        }
    };
} // namespace NKikimr::NSqsTopic::V1

namespace NKikimr::NGRpcService {

    using namespace NSqsTopic::V1;

#define DECLARE_RPC(name)                                                                           \
    template <>                                                                                     \
    IActor* TEvSqsTopic##name##Request::CreateRpcActor(NKikimr::NGRpcService::IRequestOpCtx* msg) { \
        return Create##name##Actor(msg).release();                                                  \
    }

#define DECLARE_RPC_NI(name)                                                                            \
    template <>                                                                                         \
    IActor* TEvSqsTopic##name##Request::CreateRpcActor(NKikimr::NGRpcService::IRequestOpCtx* msg) {     \
        return new TNotImplementedRequestActor<NKikimr::NGRpcService::TEvSqsTopic##name##Request>(msg); \
    }

    DECLARE_RPC(ChangeMessageVisibility);
    DECLARE_RPC(ChangeMessageVisibilityBatch);
    DECLARE_RPC(DeleteMessage);
    DECLARE_RPC(DeleteMessageBatch);
    DECLARE_RPC(GetQueueUrl);
    DECLARE_RPC(GetQueueAttributes);
    DECLARE_RPC(ListQueues);
    DECLARE_RPC(ReceiveMessage);
    DECLARE_RPC(SendMessage);
    DECLARE_RPC(SendMessageBatch);
    DECLARE_RPC(CreateQueue);
    DECLARE_RPC(SetQueueAttributes);
    DECLARE_RPC(PurgeQueue);
    DECLARE_RPC(DeleteQueue);
    DECLARE_RPC_NI(ListDeadLetterSourceQueues);
    DECLARE_RPC_NI(ListQueueTags);
    DECLARE_RPC_NI(TagQueue);
    DECLARE_RPC_NI(UntagQueue);

#undef DECLARE_RPC
#undef DECLARE_RPC_NI
} // namespace NKikimr::NGRpcService
