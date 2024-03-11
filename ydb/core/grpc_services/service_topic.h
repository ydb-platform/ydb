#pragma once

#include <memory>

namespace NKikimr {

namespace NGRpcProxy::V1 {
class IClustersCfgProvider;
struct TClustersCfg;
}

namespace NGRpcService {

class IRequestOpCtx;
class IFacilityProvider;

void DoDropTopicRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoCreateTopicRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f, TIntrusiveConstPtr<NGRpcProxy::V1::TClustersCfg>);
void DoAlterTopicRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoDescribeTopicRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoDescribeConsumerRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoDescribePartitionRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);

void DoPQDropTopicRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoPQCreateTopicRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f, TIntrusiveConstPtr<NGRpcProxy::V1::TClustersCfg>);
void DoPQAlterTopicRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f, TIntrusiveConstPtr<NGRpcProxy::V1::TClustersCfg>);
void DoPQDescribeTopicRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoPQAddReadRuleRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoPQRemoveReadRuleRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);

}
}
