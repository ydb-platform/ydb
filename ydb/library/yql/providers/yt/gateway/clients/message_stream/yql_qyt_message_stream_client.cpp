#include "yql_qyt_message_stream_client.h"
#include "yql_qyt_blocking_queue.h"

#include <atomic>
#include <functional>
#include <future>

#include <library/cpp/threading/future/async.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/queue_client.h>
#include <yt/yt/client/api/transaction.h>
#include <yt/yt/client/transaction_client/helpers.h>
#include <yt/yt/client/queue_client/consumer_client.h>
#include <yt/yt/client/queue_client/queue_rowset.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/actions/future.h>
#include <library/cpp/yt/logging/logger.h>

#include <util/generic/yexception.h>
#include <util/string/builder.h>

#include <util/datetime/base.h>

#include <map>
#include <mutex>
#include <thread>

#include "yql_qyt_read_session.h"
namespace NYql {
namespace {
using namespace NFq;
TString JoinYtPath(const TString& prefix, const TString& path) {
    if (prefix.empty()) {
        return path;
    }
    if (path.StartsWith('/')) {
        return path;
    }
    TStringBuilder result;
    result << prefix;
    if (!prefix.EndsWith('/')) {
        result << '/';
    }
    result << path;
    return result;
}

////////////////////////////////////////////////////////////////////////////////

class TQytMessageStreamClient final : public IMessageStreamClient, public std::enable_shared_from_this<TQytMessageStreamClient> {
    public:
    TQytMessageStreamClient(const TString& stream, const TQytMessageStreamClientSettings& settings)
        : Settings(settings)
        , Stream(stream)
    {
        if (Stream.empty()) {
            ythrow TMessageStreamException(EMessageStreamStatus::InvalidArgument) << "QYT stream must be nonempty";
        }
        Y_ENSURE(Settings.Client, "YT client must be provided for YT topic client");
    }

    const TString& GetStream() const final { return Stream; }

    NThreading::TFuture<TMessageStreamResult<TMessageStreamDescription>> DescribeStream() final {
        const auto& path = Stream;
        try {
            TMessageStreamDescription description;
            const auto count = GetTabletCount(path);
            for (i64 id = 0; id < count; ++id) {
                description.Partitions.push_back({NFq::TMessageStreamPartitionId{static_cast<ui64>(id)}, true});
            }
            return MakeResult(std::move(description));
        } catch (const std::exception& ex) {
            return MakeError<TMessageStreamDescription>(EMessageStreamStatus::InternalError,
                TStringBuilder() << "Failed to describe YT queue " << path << ": " << ex.what());
        }
    }

    NThreading::TFuture<TMessageStreamResult<TMessageStreamConsumerDescription>> DescribeConsumer(
        const TString& consumer, const TMessageStreamDescribeConsumerSettings& settings) final
    {
        const auto& path = Stream;
        try {
            NYT::NYPath::TRichYPath queuePath(ResolvePath(path));
            NYT::NYPath::TRichYPath consumerPath(ResolvePath(consumer));
            auto registrations = NYT::NConcurrency::WaitFor(
                Settings.Client->ListQueueConsumerRegistrations(queuePath, consumerPath, {})).ValueOrThrow();
            if (registrations.empty()) {
                return MakeError<TMessageStreamConsumerDescription>(EMessageStreamStatus::NotFound,
                    TStringBuilder() << "Consumer " << consumer << " is not registered for queue " << path);
            }

            TMessageStreamConsumerDescription description;
            const auto tabletCount = GetTabletCount(path);
            for (i64 partition = 0; partition < tabletCount; ++partition) {
                TMessageStreamConsumerPartition info;
                info.PartitionId = NFq::TMessageStreamPartitionId{static_cast<ui64>(partition)};
                info.CommittedOffset = GetConsumerOffset(consumerPath, queuePath, partition);
                if (settings.IncludeStats) {
                    auto stats = GetPartitionDescription(queuePath, partition);
                    info.StartOffset = stats.StartOffset;
                    info.EndOffset = stats.EndOffset;
                }
                description.Partitions.push_back(std::move(info));
            }
            return MakeResult(std::move(description));
        } catch (const std::exception& ex) {
            return MakeError<TMessageStreamConsumerDescription>(EMessageStreamStatus::InternalError,
                TStringBuilder() << "Failed to describe consumer " << consumer << " for queue " << path << ": " << ex.what());
        }
    }

    NThreading::TFuture<TMessageStreamResult<TMessageStreamPartitionDescription>> DescribePartition(
        NFq::TMessageStreamPartitionId id) final
    {
        const auto& path = Stream;
        const ui64 partitionId = id.Value;
        try {
            const auto tabletCount = GetTabletCount(path);
            if (partitionId >= static_cast<ui64>(tabletCount)) {
                return MakeError<TMessageStreamPartitionDescription>(EMessageStreamStatus::NotFound,
                    TStringBuilder() << "Partition " << partitionId << " does not exist for queue " << path);
            }
            return MakeResult(GetPartitionDescription(NYT::NYPath::TRichYPath(ResolvePath(path)), partitionId));
        } catch (const std::exception& ex) {
            return MakeError<TMessageStreamPartitionDescription>(EMessageStreamStatus::InternalError,
                TStringBuilder() << "Failed to describe partition " << partitionId << " for queue " << path << ": " << ex.what());
        }
    }

    std::shared_ptr<IMessageStreamReadSession> CreateReadSession(const TMessageStreamReadSessionSettings& settings) final {

        settings.Validate();
        if (!settings.Consumer || settings.PartitionIds.empty() || settings.Retry ||
            settings.OffsetResetPolicy != EMessageStreamOffsetResetPolicy::Earliest) {
            ythrow TMessageStreamException(EMessageStreamStatus::Unsupported)
                << "QYT requires a consumer, explicit partitions, and the default retry policy";
        }
        if (settings.ReadFromWriteTime && *settings.ReadFromWriteTime != TInstant::Zero()) {
            ythrow TMessageStreamException(EMessageStreamStatus::Unsupported)
                << "QYT does not support seeking by write time";
        }
        const auto memoryLimit = settings.MaxMemoryUsageBytes ? settings.MaxMemoryUsageBytes : (16ULL << 20);
        auto events = std::make_shared<TQytEventQueue>(memoryLimit);
        std::vector<std::shared_ptr<IMessageStreamReadSession>> sessions;
        for (auto partition : settings.PartitionIds) {
            auto single = settings;
            single.PartitionIds = {partition};
            sessions.push_back(CreatePartitionSession(single, events));
        }
        return CreateQytMultiReadSession(std::move(sessions));
    }

    std::shared_ptr<IMessageStreamReadSession> CreatePartitionSession(
        const TMessageStreamReadSessionSettings& settings, const std::shared_ptr<TQytEventQueue>& events)
    {
        const TString& topicPath = Stream;
        const ui64 partitionId = settings.PartitionIds.front().Value;
        Y_ENSURE(partitionId <= static_cast<ui64>(std::numeric_limits<int>::max()), "Invalid QYT partition");
        const int partitionIndex = static_cast<int>(partitionId);

        NYT::NYPath::TRichYPath queuePath(ResolvePath(topicPath));
        NYT::NYPath::TRichYPath consumerPath(ResolvePath(*settings.Consumer));


        // Get consumer offset for starting position.
        const i64 consumerOffset = GetConsumerOffset(consumerPath, queuePath, partitionIndex);
        const auto bounds = GetPartitionDescription(queuePath, partitionIndex);
        const i64 startOffset = std::max<ui64>(consumerOffset, *bounds.StartOffset);

        auto commit = [client = shared_from_this(), consumer = *settings.Consumer, partitionId](ui64 offset) {
            return client->CommitPosition(NFq::TMessageStreamPartitionId{partitionId}, consumer, offset).Apply([](const auto& future) {
                const auto& result = future.GetValue();
                Y_ENSURE(result.IsSuccess(), result.Issues.ToString());
            });
        };
        return CreateQytPartitionReadSession(Settings, settings, queuePath, consumerPath,
            partitionIndex, startOffset, *bounds.StartOffset, *bounds.EndOffset, std::move(commit), events);
    }

    NThreading::TFuture<TMessageStreamResult<TMessageStreamConsumerPosition>> CommitPosition(
        NFq::TMessageStreamPartitionId partitionId, const TString& consumerName, ui64 nextOffset) final {
        const auto& path = Stream;
        if (partitionId.Value > static_cast<ui64>(std::numeric_limits<int>::max()) ||
            nextOffset > static_cast<ui64>(std::numeric_limits<i64>::max())) {
            return MakeError<TMessageStreamConsumerPosition>(EMessageStreamStatus::InvalidArgument, "Invalid QYT consumer position");
        }
        try {
            NYT::NYPath::TRichYPath queuePath(ResolvePath(path));
            NYT::NYPath::TRichYPath consumerPath(ResolvePath(consumerName));
            const int partition = static_cast<int>(partitionId.Value);
            const i64 currentOffset = GetConsumerOffset(consumerPath, queuePath, partition);
            auto txn = NYT::NConcurrency::WaitFor(Settings.Client->StartTransaction(
                NYT::NTransactionClient::ETransactionType::Tablet)).ValueOrThrow();
            NYT::NConcurrency::WaitFor(txn->AdvanceQueueConsumer(consumerPath, queuePath, partition,
                std::optional<i64>(currentOffset), static_cast<i64>(nextOffset))).ThrowOnError();
            NYT::NConcurrency::WaitFor(txn->Commit()).ThrowOnError();
            return MakeResult(TMessageStreamConsumerPosition{partitionId, nextOffset});
        } catch (const std::exception& ex) {
            return MakeError<TMessageStreamConsumerPosition>(EMessageStreamStatus::InternalError,
                TStringBuilder() << "YT advance consumer failed: " << ex.what());
        }
    }

private:
    TString ResolvePath(const TString& path) const {
        return JoinYtPath(Settings.PathPrefix, path);
    }

    i64 GetConsumerOffset(const NYT::NYPath::TRichYPath& consumerPath, const NYT::NYPath::TRichYPath& queuePath, int partitionIndex) {

        auto subConsumer = NYT::NQueueClient::CreateSubConsumerClient(
            Settings.Client, Settings.Client, consumerPath, queuePath);
        auto partitions = NYT::NConcurrency::WaitFor(
            subConsumer->CollectPartitions(std::vector<int>{partitionIndex})).ValueOrThrow();

        for (const auto& partition : partitions) {
            if (partition.PartitionIndex == partitionIndex) {
                return partition.NextRowIndex < 0 ? 0 : partition.NextRowIndex;
            }
        }
        return 0;
    }

    i64 GetTabletCount(const TString& path) {
        const auto attrPath = ResolvePath(path) + "/@tablet_count";
        auto node = NYT::NConcurrency::WaitFor(
            Settings.Client->GetNode(attrPath)).ValueOrThrow();
        return NYT::NYTree::ConvertTo<i64>(node);
    }

    TMessageStreamPartitionDescription GetPartitionDescription(const NYT::NYPath::TRichYPath& queuePath, int partitionIndex) {
        auto tabletInfos = NYT::NConcurrency::WaitFor(
            Settings.Client->GetTabletInfos(queuePath.GetPath(), std::vector<int>{partitionIndex})).ValueOrThrow();
        Y_ENSURE(!tabletInfos.empty(), "No tablet info for QYT partition " << partitionIndex);
        TMessageStreamPartitionDescription result;
        result.PartitionId = NFq::TMessageStreamPartitionId{static_cast<ui64>(partitionIndex)};
        result.StartOffset = tabletInfos.front().TrimmedRowCount;
        result.EndOffset = tabletInfos.front().TotalRowCount;
        return result;
    }

    template <class TValue>
    static NThreading::TFuture<TMessageStreamResult<TValue>> MakeResult(TValue value) {
        return NThreading::MakeFuture(TMessageStreamResult<TValue>{EMessageStreamStatus::Success, {}, std::move(value)});
    }

    template <class TValue>
    static NThreading::TFuture<TMessageStreamResult<TValue>> MakeError(EMessageStreamStatus status, const TString& message) {
        TMessageStreamResult<TValue> result;
        result.Status = status;
        result.Issues.AddIssue(message);
        return NThreading::MakeFuture(std::move(result));
    }

    const TQytMessageStreamClientSettings Settings;
    const TString Stream;


};

} // anonymous namespace

std::shared_ptr<NFq::IMessageStreamClient> CreateQytMessageStreamClient(const TString& stream, const TQytMessageStreamClientSettings& settings) {
    return std::make_shared<TQytMessageStreamClient>(stream, settings);
}

} // namespace NYql
