#include "topic_pqrb_metrics.h"

#include <ydb/core/persqueue/public/describer/describer.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr::NSQS {

namespace {

class TTopicPqrbMetricsSenderActor : public NActors::TActorBootstrapped<TTopicPqrbMetricsSenderActor> {
public:
    TTopicPqrbMetricsSenderActor(
        TString databasePath,
        TString topicPath,
        NKikimrPQ::TEvTopicSqsActionMetrics metrics
    )
        : DatabasePath_(std::move(databasePath))
        , TopicPath_(std::move(topicPath))
        , Metrics_(std::move(metrics))
    {
    }

    void Bootstrap() {
        NPQ::NDescriber::TDescribeSettings settings = {
            .AccessRights = NACLib::EAccessRights::DescribeSchema,
        };
        DescriberActorId_ = RegisterWithSameMailbox(
            NPQ::NDescriber::CreateDescriberActor(SelfId(), DatabasePath_, {TopicPath_}, settings)
        );
        Become(&TTopicPqrbMetricsSenderActor::DescribeState);
    }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::SQS_ACTOR;
    }

private:
    STATEFN(DescribeState) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NPQ::NDescriber::TEvDescribeTopicsResponse, HandleDescribeTopicsResponse);
            cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);
        }
    }

    void HandleDescribeTopicsResponse(NPQ::NDescriber::TEvDescribeTopicsResponse::TPtr& ev) {
        DescriberActorId_ = {};

        const auto it = ev->Get()->Topics.find(TopicPath_);
        if (it == ev->Get()->Topics.end() || it->second.Status != NPQ::NDescriber::EStatus::SUCCESS || !it->second.Info) {
            PassAway();
            return;
        }

        const ui64 balancerTabletId = it->second.Info->Description.GetBalancerTabletID();
        SendTopicSqsActionMetricsToPqrb(balancerTabletId, Metrics_);
        PassAway();
    }

    void PassAway() override {
        if (DescriberActorId_) {
            Send(DescriberActorId_, new NActors::TEvents::TEvPoison());
        }
        TActorBootstrapped::PassAway();
    }

    TString DatabasePath_;
    TString TopicPath_;
    NKikimrPQ::TEvTopicSqsActionMetrics Metrics_;
    NActors::TActorId DescriberActorId_;
};

} // namespace

NActors::IActor* CreateTopicPqrbMetricsSender(
    TString databasePath,
    TString topicPath,
    NKikimrPQ::TEvTopicSqsActionMetrics metrics
) {
    return new TTopicPqrbMetricsSenderActor(std::move(databasePath), std::move(topicPath), std::move(metrics));
}

void SendTopicPqrbMetrics(
    ui64 balancerTabletId,
    const TString& databasePath,
    const TString& topicPath,
    NKikimrPQ::TEvTopicSqsActionMetrics metrics
) {
    if (!HasTopicSqsActionMetrics(metrics)) {
        return;
    }

    if (balancerTabletId != 0) {
        SendTopicSqsActionMetricsToPqrb(balancerTabletId, metrics);
        return;
    }

    if (databasePath.empty() || topicPath.empty()) {
        return;
    }

    NActors::TActivationContext::Register(CreateTopicPqrbMetricsSender(databasePath, topicPath, std::move(metrics)));
}

} // namespace NKikimr::NSQS
