#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/impl/topic_impl.h>
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/table_helpers/helpers.h>

#include <ydb/library/persqueue/obfuscate/obfuscate.h>

#include <util/random/random.h>
#include <util/string/cast.h>
#include <util/string/subst.h>

namespace NYdb::NTopic {

TTopicClient::TTopicClient(const TDriver& driver, const TTopicClientSettings& settings)
    : Impl_(std::make_shared<TImpl>(CreateInternalInterface(driver), settings))
{
}

TDescribeTopicResult::TDescribeTopicResult(TStatus&& status, Ydb::Topic::DescribeTopicResult&& result)
    : TStatus(std::move(status))
    , TopicDescription_(std::move(result))
{
}

const TTopicDescription& TDescribeTopicResult::GetTopicDescription() const {
    return TopicDescription_;
}

TTopicDescription::TTopicDescription(Ydb::Topic::DescribeTopicResult&& result)
    : Proto_(std::move(result))
    , PartitioningSettings_(Proto_.partitioning_settings())
    , RetentionPeriod_(TDuration::Seconds(Proto_.retention_period().seconds()))
    , RetentionStorageMb_(Proto_.retention_storage_mb() > 0 ? TMaybe<ui64>(Proto_.retention_storage_mb()) : Nothing())
    , PartitionWriteSpeedBytesPerSecond_(Proto_.partition_write_speed_bytes_per_second())
    , PartitionWriteBurstBytes_(Proto_.partition_write_burst_bytes())
{
    Owner_ = Proto_.self().owner();
    PermissionToSchemeEntry(Proto_.self().permissions(), &Permissions_);
    PermissionToSchemeEntry(Proto_.self().effective_permissions(), &EffectivePermissions_);

    for (const auto& part : Proto_.partitions()) {
        Partitions_.emplace_back(part);
    }
    for (const auto& codec : Proto_.supported_codecs().codecs()) {
        SupportedCodecs_.push_back((ECodec)codec);
    }
    for (const auto& pair : Proto_.attributes()) {
        Attributes_[pair.first] = pair.second;
    }
    for (const auto& consumer : Proto_.consumers()) {
        Consumers_.emplace_back(consumer);
    }
}

const TPartitioningSettings& TTopicDescription::GetPartitioningSettings() const {
    return PartitioningSettings_;
}

ui32 TTopicDescription::GetTotalPartitionsCount() const {
    return Partitions_.size();
}

const TVector<TPartitionInfo>&  TTopicDescription::GetPartitions() const {
    return Partitions_;
}

const TVector<ECodec>& TTopicDescription::GetSupportedCodecs() const {
    return SupportedCodecs_;
}

const TDuration& TTopicDescription::GetRetentionPeriod() const {
    return RetentionPeriod_;
}

TMaybe<ui64> TTopicDescription::GetRetentionStorageMb() const {
    return RetentionStorageMb_;
}

ui64 TTopicDescription::GetPartitionWriteSpeedBytesPerSecond() const {
    return PartitionWriteSpeedBytesPerSecond_;
}

ui64 TTopicDescription::GetPartitionWriteBurstBytes() const {
    return PartitionWriteBurstBytes_;
}

const TMap<TString, TString>& TTopicDescription::GetAttributes() const {
    return Attributes_;
}

const TVector<TConsumer>& TTopicDescription::GetConsumers() const {
    return Consumers_;
}

void TTopicDescription::SerializeTo(Ydb::Topic::CreateTopicRequest& request) const {
    Y_UNUSED(request);
    Y_FAIL("Not implemented");
}

const Ydb::Topic::DescribeTopicResult& TTopicDescription::GetProto() const {
    return Proto_;
}

const TString& TTopicDescription::GetOwner() const {
    return Owner_;
}

const TVector<NScheme::TPermissions>& TTopicDescription::GetPermissions() const {
    return Permissions_;
}

const TVector<NScheme::TPermissions>& TTopicDescription::GetEffectivePermissions() const {
    return EffectivePermissions_;
}

TPartitioningSettings::TPartitioningSettings(const Ydb::Topic::PartitioningSettings& settings)
    : MinActivePartitions_(settings.min_active_partitions())
    , PartitionCountLimit_(settings.partition_count_limit())
{}

ui64 TPartitioningSettings::GetMinActivePartitions() const {
    return MinActivePartitions_;
}

ui64 TPartitioningSettings::GetPartitionCountLimit() const {
    return PartitionCountLimit_;
}

TConsumer::TConsumer(const Ydb::Topic::Consumer& consumer)
    : ConsumerName_(consumer.name())
    , Important_(consumer.important())
    , ReadFrom_(TInstant::Seconds(consumer.read_from().seconds()))
{
    for (const auto& codec : consumer.supported_codecs().codecs()) {
        SupportedCodecs_.push_back((ECodec)codec);
    }
    for (const auto& pair : consumer.attributes()) {
        Attributes_[pair.first] = pair.second;
    }
}

TPartitionInfo::TPartitionInfo(const Ydb::Topic::DescribeTopicResult::PartitionInfo& partitionInfo)
    : PartitionId_(partitionInfo.partition_id())
    , Active_(partitionInfo.active())
{
    for (const auto& partId : partitionInfo.child_partition_ids()) {
        ChildPartitionIds_.push_back(partId);
    }

    for (const auto& partId : partitionInfo.parent_partition_ids()) {
        ParentPartitionIds_.push_back(partId);
    }
}


TAsyncStatus TTopicClient::CreateTopic(const TString& path, const TCreateTopicSettings& settings) {
    return Impl_->CreateTopic(path, settings);
}


TAsyncStatus TTopicClient::AlterTopic(const TString& path, const TAlterTopicSettings& settings) {
    return Impl_->AlterTopic(path, settings);
}

TAsyncStatus TTopicClient::DropTopic(const TString& path, const TDropTopicSettings& settings) {
    return Impl_->DropTopic(path, settings);
}

TAsyncDescribeTopicResult TTopicClient::DescribeTopic(const TString& path, const TDescribeTopicSettings& settings) {
    return Impl_->DescribeTopic(path, settings);
}

} // namespace NYdb::NTopic
