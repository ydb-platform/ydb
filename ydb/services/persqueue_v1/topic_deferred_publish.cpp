#include "topic_deferred_publish.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/service_topic_deferred_publish.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/library/grpc/server/grpc_method_setup.h>

namespace NKikimr::NGRpcService::V1 {

void TGRpcTopicDeferredPublishService::InitService(grpc::ServerCompletionQueue* cq, NYdbGrpc::TLoggerPtr logger) {
    CQ_ = cq;

    if (ActorSystem_->AppData<TAppData>()->PQConfig.GetEnabled()) {
        SetupIncomingRequests(std::move(logger));
    }
}

void TGRpcTopicDeferredPublishService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    using namespace Ydb::Topic::DeferredPublish;
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);

#ifdef SETUP_TOPIC_DEFERRED_PUBLISH_METHOD
#error SETUP_TOPIC_DEFERRED_PUBLISH_METHOD macro already defined
#endif

#define SETUP_TOPIC_DEFERRED_PUBLISH_METHOD(methodName, methodCallback) \
    SETUP_METHOD(methodName, methodCallback, RLSWITCH(Rps), UNSPECIFIED, topic_deferred_publish, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Dml), EEmptyDatabaseMode::EmptyDatabaseForbidden)

    SETUP_TOPIC_DEFERRED_PUBLISH_METHOD(BeginPublication, DoBeginPublicationRequest);
    SETUP_TOPIC_DEFERRED_PUBLISH_METHOD(Publish, DoPublishRequest);
    SETUP_TOPIC_DEFERRED_PUBLISH_METHOD(CancelPublication, DoCancelPublicationRequest);
    SETUP_TOPIC_DEFERRED_PUBLISH_METHOD(ListPublications, DoListPublicationsRequest);
    SETUP_TOPIC_DEFERRED_PUBLISH_METHOD(DescribePublication, DoDescribePublicationRequest);

#undef SETUP_TOPIC_DEFERRED_PUBLISH_METHOD
}

}
