#pragma once

#include "schema.h"

#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/persqueue/public/describer/describer.h>
#include <ydb/library/actors/core/actorsystem_fwd.h>
#include <ydb/public/api/protos/persqueue_error_codes_v1.pb.h>
#include <ydb/public/api/protos/ydb_topic.pb.h>

#include <absl/container/flat_hash_map.h>

#include <memory>
#include <optional>

namespace NKikimr::NPQ::NSchema {

struct TDescribeOperationSettings {
    TString Path;
    TString Database;
    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    NDescriber::TAccessRights AccessRights;
    bool IncludeStats = false;
    bool IncludeLocation = false;
    bool ForceSyncVersion = false;
};

struct TPartitionDescribeInfo {
    Ydb::Topic::PartitionLocation Location;
    Ydb::Topic::DescribeConsumerResult::PartitionInfo Stats;
    NKikimrPQ::TReadSessionsInfoResponse::TPartitionInfo ReadSession;
    absl::flat_hash_map<TString, Ydb::Topic::Consumer::ConsumerStats> Consumers;
};

struct TEvDescribeOperationResponse
    : public NActors::TEventLocal<TEvDescribeOperationResponse, EEv::EvDescribeOperationResponse>
{
    Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::SUCCESS;
    TString ErrorMessage;
    Ydb::PersQueue::ErrorCode::ErrorCode IssueCode = Ydb::PersQueue::ErrorCode::OK;

    NDescriber::TTopicInfo TopicInfo;
    Ydb::Scheme::Entry SelfEntry;
    absl::flat_hash_map<ui32, TPartitionDescribeInfo> Partitions;
    TString ConsumerName;
    bool UsedSyncVersion = false;
};

struct TDescribeSchemaError {
    Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::BAD_REQUEST;
    TString Message;
    Ydb::PersQueue::ErrorCode::ErrorCode IssueCode = Ydb::PersQueue::ErrorCode::BAD_REQUEST;
    bool RetryWithSync = false;
};

struct TDescribeSchemaResult {
    std::optional<TDescribeSchemaError> Error;
    TString ConsumerName;
};

class IDescribeStrategy {
public:
    virtual ~IDescribeStrategy() = default;

    virtual TString GetName() const = 0;
    virtual TDescribeSchemaResult ValidateSchema(const NDescriber::TTopicInfo& topicInfo) = 0;
    virtual bool NeedProcessPartition(
        const NKikimrSchemeOp::TPersQueueGroupDescription::TPartition& partition) const = 0;
    virtual std::unique_ptr<TEvPersQueue::TEvGetReadSessionsInfo> CreateReadSessionsInfoRequest() const = 0;
    virtual std::unique_ptr<TEvPersQueue::TEvStatus> CreateStatusRequest() const = 0;
};

NActors::IActor* CreateDescribeOperationActor(
    const NActors::TActorId& parent,
    TDescribeOperationSettings&& settings,
    std::unique_ptr<IDescribeStrategy> strategy);

} // namespace NKikimr::NPQ::NSchema
