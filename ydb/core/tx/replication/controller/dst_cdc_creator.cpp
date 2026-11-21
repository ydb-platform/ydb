#include "dst_cdc_creator.h"
#include "dst_creator.h"
#include "topic_mirror.h"

#include <ydb/core/base/path.h>
#include <ydb/core/persqueue/public/schema/schema_propose.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/ydb_convert/table_description.h>

#include <google/protobuf/util/message_differencer.h>
#include <util/generic/algorithm.h>
#include <util/generic/hash_set.h>

#include <utility>

namespace NKikimr::NReplication::NController {

bool TCdcCreation::Prepare(const NYdb::NTopic::TTopicDescription& topic, const TString& database, TString& error) {
    const auto& source = Settings.SourceStream;
    if (!EqualToOneOf(source.state(), Ydb::Table::ChangefeedDescription::STATE_ENABLED,
            Ydb::Table::ChangefeedDescription::STATE_DISABLED)
        || source.name().empty() || source.name().find('/') != TString::npos
        || source.attributes().find("__async_replication") != source.attributes().end()
        || source.schema_changes() || source.virtual_timestamps()
        || source.has_resolved_timestamps_interval())
    {
        error = "CDC mirroring requires a user Enabled/Disabled stream without schema changes or timestamps";
        return false;
    }

    Ydb::StatusIds::StatusCode status;
    if (!FillChangefeedDescription(*Definition.MutableStreamDescription(), source, status, error)) {
        return false;
    }
    Definition.MutableStreamDescription()->SetState(NKikimrSchemeOp::ECdcStreamStateDisabled);

    const auto marker = topic.GetAttributes().find("_ydb_replication_consumer");
    const auto consumer = FindIf(topic.GetConsumers(), [&](const auto& value) {
        return value.GetConsumerName() == Settings.Consumer;
    });
    if (Settings.Consumer.empty() || marker == topic.GetAttributes().end() || marker->second != Settings.Consumer
        || consumer == topic.GetConsumers().end() || !consumer->GetImportant())
    {
        error = "CDC source consumer must be registered before destination creation";
        return false;
    }

    const auto& partitions = topic.GetPartitions();
    THashSet<ui64> ids;
    if (partitions.empty()
        || topic.GetPartitioningSettings().GetAutoPartitioningSettings().GetStrategy()
            != NYdb::NTopic::EAutoPartitioningStrategy::Disabled)
    {
        error = "CDC mirroring requires fixed nonempty source partitions";
        return false;
    }
    for (const auto& partition : partitions) {
        if (!partition.GetActive() || partition.GetPartitionId() >= partitions.size()
            || !partition.GetParentPartitionIds().empty() || !partition.GetChildPartitionIds().empty()
            || !ids.insert(partition.GetPartitionId()).second)
        {
            error = "CDC mirroring requires unsplit contiguous source partitions";
            return false;
        }
    }

    Definition.SetTopicPartitions(partitions.size());
    ActivateOnDetach = source.state() == Ydb::Table::ChangefeedDescription::STATE_ENABLED;
    auto request = BuildTopicMirrorRequest(topic, Settings.Consumer);
    request.mutable_partitioning_settings()->set_min_active_partitions(Definition.GetTopicPartitions());

    NKikimrSchemeOp::TModifyScheme proposal;
    auto result = NPQ::NSchema::ProposeCreateTopic(proposal, std::move(request),
        database, database, Definition.GetStreamDescription().GetName());
    if (!result) {
        error = result.GetErrorMessage();
        return false;
    }

    const auto& config = proposal.GetCreatePersQueueGroup().GetPQTabletConfig();
    Definition.SetRetentionPeriodSeconds(config.GetPartitionConfig().GetLifetimeSeconds());
    Topic.SetReplicationSourceConsumer(Settings.Consumer);
    *Topic.MutableConsumers() = config.GetConsumers();
    *Topic.MutablePartitionConfig()->MutableReadQuota() = config.GetPartitionConfig().GetReadQuota();
    if (AnyOf(Topic.GetConsumers(), [](const auto& consumer) {
        return consumer.GetMetricsLevel() || !consumer.GetMonitoringProjectId().empty();
    })) {
        error = "CDC consumer monitoring overrides cannot be migrated yet";
        return false;
    }
    return true;
}

bool TCdcCreation::CheckTable(const NKikimrScheme::TEvDescribeSchemeResult& record,
        const TPathId& replicationPathId, ui64 schemeShardId, TString& owner, TString& error)
{
    const auto& desc = record.GetPathDescription();
    const auto& self = desc.GetSelf();
    const TPathId id(self.GetSchemeshardId(), self.GetPathId());
    const auto& config = desc.GetTable().GetReplicationConfig();

    const bool ownedReplica = config.HasOwnerPathId() && TPathId::FromProto(config.GetOwnerPathId()) == replicationPathId
        && config.GetImportState() != NKikimrSchemeOp::TTableReplicationConfig::WAITING_IMPORT
        && CheckReplicationConfig(config, EReplicationMode::ReadOnly, EConsistencyLevel::Row, error);
    if (!replicationPathId || !Settings.TablePathId || id != Settings.TablePathId
        || id.OwnerId != schemeShardId || !desc.HasTable() || !ownedReplica)
    {
        error = "CDC destination must be the final owned ROW replica";
        return false;
    }

    if (Definition.HasTableName() && Definition.GetTableName() != self.GetName()) {
        error = "CDC parent name changed";
        return false;
    }
    Settings.StreamPathId = {};
    Settings.TopicPathId = {};

    TableVersion = self.GetPathVersion();
    TablePartitionCount = desc.GetTable().GetPartitionCount();
    owner = self.GetOwner();
    TablePath = record.GetPath();
    Definition.SetTableName(self.GetName());
    return true;
}

bool TCdcCreation::CheckStream(const NKikimrScheme::TEvDescribeSchemeResult& record, ui64 schemeShardId,
        const TString& owner, TString& error)
{
    const auto& desc = record.GetPathDescription();
    const auto& self = desc.GetSelf();
    const TPathId id(self.GetSchemeshardId(), self.GetPathId());
    Ydb::Table::ChangefeedDescription expected, actual;
    FillChangefeedDescription(expected, Definition.GetStreamDescription());
    FillChangefeedDescription(actual, desc.GetCdcStreamDescription());
    if (!desc.HasCdcStreamDescription() || self.GetParentPathId() != Settings.TablePathId.LocalPathId
        || id.OwnerId != schemeShardId
        || (Settings.StreamPathId && id != Settings.StreamPathId)
        || !google::protobuf::util::MessageDifferencer::Equivalent(expected, actual))
    {
        error = "CDC destination stream identity or configuration changed";
        return false;
    }
    if (self.GetOwner() != owner || !self.GetACL().empty()) {
        error = "CDC destination requires inherited owner and ACL";
        return false;
    }
    if (InitialCreateTxId
        && (self.GetCreateTxId() != InitialCreateTxId || self.GetVersion().GetCdcStreamVersion() != 1))
    {
        error = "CDC stream was not created by the saved creation request";
        return false;
    }
    Settings.StreamPathId = id;
    return true;
}

bool TCdcCreation::CheckTopic(const NKikimrScheme::TEvDescribeSchemeResult& record, ui64 schemeShardId,
        const TString& owner, TString& error)
{
    const auto& desc = record.GetPathDescription();
    const auto& self = desc.GetSelf();
    const TPathId id(self.GetSchemeshardId(), self.GetPathId());
    const auto& topic = desc.GetPersQueueGroup();
    const auto& config = topic.GetPQTabletConfig();
    if (!desc.HasPersQueueGroup() || self.GetParentPathId() != Settings.StreamPathId.LocalPathId
        || id.OwnerId != schemeShardId
        || (Settings.TopicPathId && id != Settings.TopicPathId)
        || config.GetPartitionConfig().HasMirrorFrom()
        || std::cmp_not_equal(config.GetPartitionConfig().GetLifetimeSeconds(), Definition.GetRetentionPeriodSeconds())
        || topic.GetTotalGroupCount() != Definition.GetTopicPartitions())
    {
        error = "CDC destination topic identity or configuration changed";
        return false;
    }
    THashSet<ui32> partitionIds;
    if (config.GetPartitionStrategy().GetPartitionStrategyType() != NKikimrPQ::TPQTabletConfig::DISABLED
        || topic.PartitionsSize() != topic.GetTotalGroupCount())
    {
        error = "CDC destination partition layout changed";
        return false;
    }
    for (const auto& partition : topic.GetPartitions()) {
        if (partition.GetStatus() != NKikimrPQ::Active
            || partition.GetPartitionId() >= topic.GetTotalGroupCount()
            || partition.ParentPartitionIdsSize() || partition.ChildPartitionIdsSize()
            || !partitionIds.insert(partition.GetPartitionId()).second)
        {
            error = "CDC destination partition layout changed";
            return false;
        }
    }
    Settings.TopicPathId = id;

    if (self.GetOwner() != owner || !self.GetACL().empty()) {
        error = "CDC destination requires inherited owner and ACL";
        return false;
    }
    if (InitialCreateTxId && self.GetCreateTxId() != InitialCreateTxId) {
        error = "CDC topic was not created by the saved creation request";
        return false;
    }
    NeedsTopicSetup = config.GetReplicationSourceConsumer().empty();
    if (NeedsTopicSetup) {
        if (config.HasCdcReplication() || self.GetVersion().GetPQVersion() != 1 || config.ConsumersSize()) {
            error = "CDC destination changed before initial topic setup";
            return false;
        }
        return true;
    }
    if (config.GetReplicationSourceConsumer() != Settings.Consumer || !config.HasCdcReplication()) {
        error = "CDC destination registration changed or was not applied";
        return false;
    }

    // The first ALTER installs the managed marker, consumers and quotas together.
    if (self.GetVersion().GetPQVersion() != 2) {
        error = "CDC topic was not created by the saved creation request";
        return false;
    }
    return true;
}

void TCdcCreation::FillTopicSetup(NKikimrSchemeOp::TModifyScheme& tx,
        const NKikimrScheme::TEvDescribeSchemeResult& record) const
{
    tx.Clear();
    tx.SetWorkingDir(TString(ExtractParent(record.GetPath())));
    tx.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterPersQueueGroup);
    auto& alter = *tx.MutableAlterPersQueueGroup();
    alter.SetName(record.GetPathDescription().GetSelf().GetName());
    alter.SetPathId(Settings.TopicPathId.LocalPathId);
    auto& config = *alter.MutablePQTabletConfig();
    config = record.GetPathDescription().GetPersQueueGroup().GetPQTabletConfig();
    config.ClearPartitionKeySchema();
    config.SetReplicationSourceConsumer(Settings.Consumer);
    *config.MutableConsumers() = Topic.GetConsumers();
    *config.MutablePartitionConfig()->MutableReadQuota() = Topic.GetPartitionConfig().GetReadQuota();

    auto& topic = *tx.AddApplyIf();
    topic.SetPathId(Settings.TopicPathId.LocalPathId);
    topic.SetPathVersion(record.GetPathDescription().GetSelf().GetPathVersion());
    auto& table = *tx.AddApplyIf();
    table.SetPathId(Settings.TablePathId.LocalPathId);
    table.SetPathVersion(TableVersion);
}

void TCdcCreation::FillCreate(NKikimrSchemeOp::TModifyScheme& tx) const {
    tx.Clear();
    tx.SetWorkingDir(TString(ExtractParent(TablePath)));
    tx.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateCdcStream);
    auto& stream = *tx.MutableCreateCdcStream();
    stream = Definition;
    // Inherit the table's split boundaries, including for non-numeric keys.
    stream.ClearTopicPartitions();
    auto& condition = *tx.AddApplyIf();
    condition.SetPathId(Settings.TablePathId.LocalPathId);
    condition.SetPathVersion(TableVersion);
}

}
