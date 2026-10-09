#pragma once
#include "yql_qyt_message_stream_client.h"
#include "yql_qyt_blocking_queue.h"
#include <yt/yt/client/ypath/rich.h>
#include <functional>
namespace NYql {
using TQytEventQueue = TBlockingEQueue<NFq::TMessageStreamReadEvent>;
std::shared_ptr<NFq::IMessageStreamReadSession> CreateQytPartitionReadSession(
    const TQytMessageStreamClientSettings& config, const NFq::TMessageStreamReadSessionSettings& settings,
    const NYT::NYPath::TRichYPath& queue, const NYT::NYPath::TRichYPath& consumer,
    int partition, ui64 start, ui64 minimum, ui64 end,
    std::function<NThreading::TFuture<void>(ui64)> commit, std::shared_ptr<TQytEventQueue> events);
std::shared_ptr<NFq::IMessageStreamReadSession> CreateQytMultiReadSession(
    std::vector<std::shared_ptr<NFq::IMessageStreamReadSession>> sessions);
}
