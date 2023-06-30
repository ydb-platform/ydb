#pragma once

#include "replication_card.h"

#include <yt/yt/core/yson/public.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt_proto/yt/client/chaos_client/proto/replication_card.pb.h>

namespace NYT::NChaosClient {

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TReplicationProgress& replicationProgress, NYson::IYsonConsumer* consumer);
void Serialize(const TReplicaHistoryItem& replicaHistoryItem, NYson::IYsonConsumer* consumer);
void Serialize(
    const TReplicaInfo& replicaInfo,
    NYTree::TFluentMap fluent,
    const TReplicationCardFetchOptions& options = {});
void Serialize(
    const TReplicaInfo& replicaInfo,
    NYson::IYsonConsumer* consumer,
    const TReplicationCardFetchOptions& options = {});
void Serialize(
    const TReplicationCard& replicationCard,
    NYson::IYsonConsumer* consumer,
    const TReplicationCardFetchOptions& options = {});
void Serialize(
    const TReplicationCard& replicationCard,
    NYTree::TFluentMap fluent,
    const TReplicationCardFetchOptions& options = {});

void Deserialize(TReplicationProgress& replicationProgress, NYTree::INodePtr node);
void Deserialize(TReplicaInfo& replicaInfo, NYTree::INodePtr node);
void Deserialize(TReplicationCard& replicationCard, NYTree::INodePtr node);

void Deserialize(TReplicationProgress& replicationProgress, NYson::TYsonPullParserCursor* cursor);
void Deserialize(TReplicaInfo& replicaInfo, NYson::TYsonPullParserCursor* cursor);
void Deserialize(TReplicationCard& replicationCard, NYson::TYsonPullParserCursor* cursor);

////////////////////////////////////////////////////////////////////////////////

void ToProto(
    NChaosClient::NProto::TReplicationProgress* protoReplicationProgress,
    const TReplicationProgress& replicationProgress);
void FromProto(
    TReplicationProgress* replicationProgress,
    const NChaosClient::NProto::TReplicationProgress& protoReplicationProgress);

void ToProto(
    NChaosClient::NProto::TReplicaInfo* protoReplicaInfo,
    const TReplicaInfo& replicaInfo,
    const TReplicationCardFetchOptions& options = {});
void FromProto(
    TReplicaInfo* replicaInfo,
    const NChaosClient::NProto::TReplicaInfo& protoReplicaInfo);

void ToProto(
    NChaosClient::NProto::TReplicationCard* protoReplicationCard,
    const TReplicationCard& replicationCard,
    const TReplicationCardFetchOptions& options = {});
void FromProto(
    TReplicationCard* replicationCard,
    const NChaosClient::NProto::TReplicationCard& protoReplicationCard);

void ToProto(
    NChaosClient::NProto::TReplicationCardFetchOptions* protoOptions,
    const TReplicationCardFetchOptions& options);
void FromProto(
    TReplicationCardFetchOptions* options,
    const NChaosClient::NProto::TReplicationCardFetchOptions& protoOptions);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient
