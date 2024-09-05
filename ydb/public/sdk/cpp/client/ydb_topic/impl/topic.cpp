#include <ydb/public/sdk/cpp/client/ydb_topic/include/client.h>

#include <ydb/public/sdk/cpp/client/ydb_topic/impl/topic_impl.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/impl/common.h>

#include <ydb/public/sdk/cpp/client/impl/ydb_internal/scheme_helpers/helpers.h>

#include <util/random/random.h>
#include <util/string/cast.h>
#include <util/string/subst.h>

namespace NYdb::NTopic {

class TCommonCodecsProvider {
public:
    TCommonCodecsProvider() {
        TCodecMap::GetTheCodecMap().Set((ui32)ECodec::GZIP, MakeHolder<TGzipCodec>());
        TCodecMap::GetTheCodecMap().Set((ui32)ECodec::ZSTD, MakeHolder<TZstdCodec>());
    }
};
TCommonCodecsProvider COMMON_CODECS_PROVIDER;

TDescribeTopicResult::TDescribeTopicResult(TStatus&& status, Ydb::Topic::DescribeTopicResult&& result)
    : TStatus(std::move(status))
    , TopicDescription_(std::move(result))
{
}

const TTopicDescription& TDescribeTopicResult::GetTopicDescription() const {
    return TopicDescription_;
}

TDescribeConsumerResult::TDescribeConsumerResult(TStatus&& status, Ydb::Topic::DescribeConsumerResult&& result)
    : TStatus(std::move(status))
    , ConsumerDescription_(std::move(result))
{
}

const TConsumerDescription& TDescribeConsumerResult::GetConsumerDescription() const {
    return ConsumerDescription_;
}

TDescribePartitionResult::TDescribePartitionResult(TStatus&& status, Ydb::Topic::DescribePartitionResult&& result)
    : TStatus(std::move(status))
    , PartitionDescription_(std::move(result))
{
}

const TPartitionDescription& TDescribePartitionResult::GetPartitionDescription() const {
    return PartitionDescription_;
}

TTopicDescription::TTopicDescription(Ydb::Topic::DescribeTopicResult&& result)
    : Proto_(std::move(result))
    , PartitioningSettings_(Proto_.partitioning_settings())
    , RetentionPeriod_(TDuration::Seconds(Proto_.retention_period().seconds()))
    , RetentionStorageMb_(Proto_.retention_storage_mb() > 0 ? TMaybe<ui64>(Proto_.retention_storage_mb()) : Nothing())
    , PartitionWriteSpeedBytesPerSecond_(Proto_.partition_write_speed_bytes_per_second())
    , PartitionWriteBurstBytes_(Proto_.partition_write_burst_bytes())
    , MeteringMode_(TProtoAccessor::FromProto(Proto_.metering_mode()))
    , TopicStats_(Proto_.topic_stats())
{
    Owner_ = Proto_.self().owner();
    CreationTimestamp_ = NScheme::TVirtualTimestamp(Proto_.self().created_at());
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

TConsumerDescription::TConsumerDescription(Ydb::Topic::DescribeConsumerResult&& result)
    : Proto_(std::move(result))
    , Consumer_(Proto_.consumer())
{
    for (const auto& part : Proto_.partitions()) {
        Partitions_.emplace_back(part);
    }
}

TPartitionDescription::TPartitionDescription(Ydb::Topic::DescribePartitionResult&& result)
    : Proto_(std::move(result))
    , Partition_(Proto_.partition())
{
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

const TString& TConsumer::GetConsumerName() const {
    return ConsumerName_;
}

bool TConsumer::GetImportant() const {
    return Important_;
}

const TInstant& TConsumer::GetReadFrom() const {
    return ReadFrom_;
}

const TVector<ECodec>& TConsumer::GetSupportedCodecs() const {
    return SupportedCodecs_;
}

const TMap<TString, TString>& TConsumer::GetAttributes() const {
    return Attributes_;
}

const TPartitioningSettings& TTopicDescription::GetPartitioningSettings() const {
    return PartitioningSettings_;
}

ui32 TTopicDescription::GetTotalPartitionsCount() const {
    return Partitions_.size();
}

const TVector<TPartitionInfo>& TTopicDescription::GetPartitions() const {
    return Partitions_;
}

const TVector<TPartitionInfo>& TConsumerDescription::GetPartitions() const {
    return Partitions_;
}

const TPartitionInfo& TPartitionDescription::GetPartition() const {
    return Partition_;
}

const TConsumer& TConsumerDescription::GetConsumer() const {
    return Consumer_;
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

EMeteringMode TTopicDescription::GetMeteringMode() const {
    return MeteringMode_;
}

const TMap<TString, TString>& TTopicDescription::GetAttributes() const {
    return Attributes_;
}

const TVector<TConsumer>& TTopicDescription::GetConsumers() const {
    return Consumers_;
}

void TTopicDescription::SerializeTo(Ydb::Topic::CreateTopicRequest& request) const {
    Y_UNUSED(request);
    Y_ABORT("Not implemented");
}

const Ydb::Topic::DescribeTopicResult& TTopicDescription::GetProto() const {
    return Proto_;
}

const Ydb::Topic::DescribeConsumerResult& TConsumerDescription::GetProto() const {
    return Proto_;
}

const Ydb::Topic::DescribePartitionResult& TPartitionDescription::GetProto() const {
    return Proto_;
}

const TString& TTopicDescription::GetOwner() const {
    return Owner_;
}

const NScheme::TVirtualTimestamp& TTopicDescription::GetCreationTimestamp() const {
    return CreationTimestamp_;
}

const TTopicStats& TTopicDescription::GetTopicStats() const {
    return TopicStats_;
}

const TVector<NScheme::TPermissions>& TTopicDescription::GetPermissions() const {
    return Permissions_;
}

const TVector<NScheme::TPermissions>& TTopicDescription::GetEffectivePermissions() const {
    return EffectivePermissions_;
}

TPartitioningSettings::TPartitioningSettings(const Ydb::Topic::PartitioningSettings& settings)
    : MinActivePartitions_(settings.min_active_partitions())
    , MaxActivePartitions_(settings.max_active_partitions())
    , PartitionCountLimit_(settings.partition_count_limit())
    , AutoPartitioningSettings_(settings.auto_partitioning_settings())
{}

ui64 TPartitioningSettings::GetMinActivePartitions() const {
    return MinActivePartitions_;
}

ui64 TPartitioningSettings::GetMaxActivePartitions() const {
    return MaxActivePartitions_;
}

ui64 TPartitioningSettings::GetPartitionCountLimit() const {
    return PartitionCountLimit_;
}

TAutoPartitioningSettings TPartitioningSettings::GetAutoPartitioningSettings() const {
    return AutoPartitioningSettings_;
}

TAutoPartitioningSettings::TAutoPartitioningSettings(const Ydb::Topic::AutoPartitioningSettings& settings)
    : Strategy_(static_cast<EAutoPartitioningStrategy>(settings.strategy()))
    , StabilizationWindow_(TDuration::Seconds(settings.partition_write_speed().stabilization_window().seconds()))
    , DownUtilizationPercent_(settings.partition_write_speed().down_utilization_percent())
    , UpUtilizationPercent_(settings.partition_write_speed().up_utilization_percent())
{}

EAutoPartitioningStrategy TAutoPartitioningSettings::GetStrategy() const {
    return Strategy_;
}

TDuration TAutoPartitioningSettings::GetStabilizationWindow() const {
    return StabilizationWindow_;
}

ui32 TAutoPartitioningSettings::GetUpUtilizationPercent() const {
    return UpUtilizationPercent_;
}

ui32 TAutoPartitioningSettings::GetDownUtilizationPercent() const {
    return DownUtilizationPercent_;
}

TTopicStats::TTopicStats(const Ydb::Topic::DescribeTopicResult::TopicStats& topicStats)
    : StoreSizeBytes_(topicStats.store_size_bytes())
    , MinLastWriteTime_(TInstant::Seconds(topicStats.min_last_write_time().seconds()))
    , MaxWriteTimeLag_(TDuration::Seconds(topicStats.max_write_time_lag().seconds()) + TDuration::MicroSeconds(topicStats.max_write_time_lag().nanos() / 1000))
    , BytesWrittenPerMinute_(topicStats.bytes_written().per_minute())
    , BytesWrittenPerHour_(topicStats.bytes_written().per_hour())
    , BytesWrittenPerDay_(topicStats.bytes_written().per_day())
{
}

ui64 TTopicStats::GetStoreSizeBytes() const {
    return StoreSizeBytes_;
}

TInstant TTopicStats::GetMinLastWriteTime() const {
    return MinLastWriteTime_;
}

TDuration TTopicStats::GetMaxWriteTimeLag() const {
    return MaxWriteTimeLag_;
}

ui64 TTopicStats::GetBytesWrittenPerMinute() const {
    return BytesWrittenPerMinute_;
}

ui64 TTopicStats::GetBytesWrittenPerHour() const {
    return BytesWrittenPerHour_;
}

ui64 TTopicStats::GetBytesWrittenPerDay() const {
    return BytesWrittenPerDay_;
}


TPartitionStats::TPartitionStats(const Ydb::Topic::PartitionStats& partitionStats)
    : StartOffset_(partitionStats.partition_offsets().start())
    , EndOffset_(partitionStats.partition_offsets().end())
    , StoreSizeBytes_(partitionStats.store_size_bytes())
    , LastWriteTime_(TInstant::Seconds(partitionStats.last_write_time().seconds()))
    , MaxWriteTimeLag_(TDuration::Seconds(partitionStats.max_write_time_lag().seconds()) + TDuration::MicroSeconds(partitionStats.max_write_time_lag().nanos() / 1000))
    , BytesWrittenPerMinute_(partitionStats.bytes_written().per_minute())
    , BytesWrittenPerHour_(partitionStats.bytes_written().per_hour())
    , BytesWrittenPerDay_(partitionStats.bytes_written().per_day())

{}

ui64 TPartitionStats::GetStartOffset() const {
    return StartOffset_;
}

ui64 TPartitionStats::GetEndOffset() const {
    return EndOffset_;
}

ui64 TPartitionStats::GetStoreSizeBytes() const {
    return StoreSizeBytes_;
}

TInstant TPartitionStats::GetLastWriteTime() const {
    return LastWriteTime_;
}

TDuration TPartitionStats::GetMaxWriteTimeLag() const {
    return MaxWriteTimeLag_;
}

ui64 TPartitionStats::GetBytesWrittenPerMinute() const {
    return BytesWrittenPerMinute_;
}

ui64 TPartitionStats::GetBytesWrittenPerHour() const {
    return BytesWrittenPerHour_;
}

ui64 TPartitionStats::GetBytesWrittenPerDay() const {
    return BytesWrittenPerDay_;
}


TPartitionConsumerStats::TPartitionConsumerStats(const Ydb::Topic::DescribeConsumerResult::PartitionConsumerStats& partitionStats)
    : CommittedOffset_(partitionStats.committed_offset())
    , LastReadOffset_(partitionStats.last_read_offset())
    , ReaderName_(partitionStats.reader_name())
    , ReadSessionId_(partitionStats.read_session_id())
{}

ui64 TPartitionConsumerStats::GetCommittedOffset() const {
    return CommittedOffset_;
}

ui64 TPartitionConsumerStats::GetLastReadOffset() const {
    return LastReadOffset_;
}

TString TPartitionConsumerStats::GetReaderName() const {
    return ReaderName_;
}

TString TPartitionConsumerStats::GetReadSessionId() const {
    return ReadSessionId_;
}

TPartitionLocation::TPartitionLocation(const Ydb::Topic::PartitionLocation& partitionLocation)
    : NodeId_(partitionLocation.node_id())
    , Generation_(partitionLocation.generation())
{
}

i32 TPartitionLocation::GetNodeId() const {
    return NodeId_;
}

i64 TPartitionLocation::GetGeneration() const {
    return Generation_;
}

TPartitionInfo::TPartitionInfo(const Ydb::Topic::DescribeTopicResult::PartitionInfo& partitionInfo)
    : PartitionId_(partitionInfo.partition_id())
    , Active_(partitionInfo.active())
    , PartitionStats_()
{
    for (const auto& partId : partitionInfo.child_partition_ids()) {
        ChildPartitionIds_.push_back(partId);
    }

    for (const auto& partId : partitionInfo.parent_partition_ids()) {
        ParentPartitionIds_.push_back(partId);
    }

    if (partitionInfo.has_partition_stats()) {
        PartitionStats_ = TPartitionStats{partitionInfo.partition_stats()};
    }

    if (partitionInfo.has_partition_location()) {
        PartitionLocation_ = TPartitionLocation{partitionInfo.partition_location()};
    }

    if (partitionInfo.has_key_range() && partitionInfo.key_range().has_from_bound()) {
        FromBound_ = TString(partitionInfo.key_range().from_bound());
    }

    if (partitionInfo.has_key_range() && partitionInfo.key_range().has_to_bound()) {
        ToBound_ = TString(partitionInfo.key_range().to_bound());
    }
}

TPartitionInfo::TPartitionInfo(const Ydb::Topic::DescribeConsumerResult::PartitionInfo& partitionInfo)
    : PartitionId_(partitionInfo.partition_id())
    , Active_(partitionInfo.active())
    , PartitionStats_()
{
    for (const auto& partId : partitionInfo.child_partition_ids()) {
        ChildPartitionIds_.push_back(partId);
    }

    for (const auto& partId : partitionInfo.parent_partition_ids()) {
        ParentPartitionIds_.push_back(partId);
    }
    if (partitionInfo.has_partition_stats()) {
        PartitionStats_ = TPartitionStats{partitionInfo.partition_stats()};
        PartitionConsumerStats_ = TPartitionConsumerStats{partitionInfo.partition_consumer_stats()};
    }
    if (partitionInfo.has_partition_location()) {
        PartitionLocation_ = TPartitionLocation{partitionInfo.partition_location()};
    }
}

const TMaybe<TPartitionStats>& TPartitionInfo::GetPartitionStats() const {
    return PartitionStats_;
}

const TMaybe<TPartitionConsumerStats>& TPartitionInfo::GetPartitionConsumerStats() const {
    return PartitionConsumerStats_;
}

const TMaybe<TPartitionLocation>& TPartitionInfo::GetPartitionLocation() const {
    return PartitionLocation_;
}

const TVector<ui64> TPartitionInfo::GetChildPartitionIds() const {
    return ChildPartitionIds_;
}

const TVector<ui64> TPartitionInfo::GetParentPartitionIds() const {
    return ParentPartitionIds_;
}

bool TPartitionInfo::GetActive() const {
    return Active_;
}

ui64 TPartitionInfo::GetPartitionId() const {
    return PartitionId_;
}

const TMaybe<TString>& TPartitionInfo::GetFromBound() const {
    return FromBound_;
}

const TMaybe<TString>& TPartitionInfo::GetToBound() const {
    return ToBound_;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TTopicClient

TTopicClient::TTopicClient(const TDriver& driver, const TTopicClientSettings& settings)
    : Impl_(std::make_shared<TImpl>(CreateInternalInterface(driver), settings))
{
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

TAsyncDescribeConsumerResult TTopicClient::DescribeConsumer(const TString& path, const TString& consumer, const TDescribeConsumerSettings& settings) {
    return Impl_->DescribeConsumer(path, consumer, settings);
}

TAsyncDescribePartitionResult TTopicClient::DescribePartition(const TString& path, i64 partitionId, const TDescribePartitionSettings& settings) {
    return Impl_->DescribePartition(path, partitionId, settings);
}

std::shared_ptr<IReadSession> TTopicClient::CreateReadSession(const TReadSessionSettings& settings) {
    return Impl_->CreateReadSession(settings);
}

std::shared_ptr<ISimpleBlockingWriteSession> TTopicClient::CreateSimpleBlockingWriteSession(
    const TWriteSessionSettings& settings) {
    return Impl_->CreateSimpleWriteSession(settings);
}

std::shared_ptr<IWriteSession> TTopicClient::CreateWriteSession(const TWriteSessionSettings& settings) {
    return Impl_->CreateWriteSession(settings);
}

TAsyncStatus TTopicClient::CommitOffset(const TString& path, ui64 partitionId, const TString& consumerName, ui64 offset,
    const TCommitOffsetSettings& settings) {
    return Impl_->CommitOffset(path, partitionId, consumerName, offset, settings);
}

}  // namespace NYdb::NTopic
