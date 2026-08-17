#pragma once

#include <ydb/core/persqueue/public/schema/common.h>
#include <ydb/public/api/protos/ydb_persqueue_v1.pb.h>

namespace NKikimr::NGRpcProxy::V1::NPQv1 {

NPQ::NSchema::TResult ApplyChangesInt(
    const TString& database,
    const TString& name,
    const Ydb::PersQueue::V1::AlterTopicRequest& request,
    NKikimrSchemeOp::TModifyScheme& modifyScheme,
    NKikimrSchemeOp::TPersQueueGroupDescription& targetConfig,
    const TString& localDc
);

NPQ::NSchema::TResult ApplyChangesInt(
    const TString& database,
    const Ydb::PersQueue::V1::CreateTopicRequest& request,
    NKikimrSchemeOp::TModifyScheme& modifyScheme,
    NKikimrSchemeOp::TPersQueueGroupDescription& targetConfig,
    const TString& localDc
);

NPQ::NSchema::TResult AddConsumer(
    NKikimrPQ::TPQTabletConfig* config,
    const Ydb::PersQueue::V1::TopicSettings::ReadRule& readRule,
    const TConsumersAdvancedMonitoringSettings* consumersAdvancedMonitoringSettings
);

} // namespace NKikimr::NGRpcProxy::V1::NPQv1
