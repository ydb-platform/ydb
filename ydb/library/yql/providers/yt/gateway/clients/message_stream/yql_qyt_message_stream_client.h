#pragma once

#include <ydb/library/yql/providers/abstract/message_stream/message_stream_client.h>

#include <yt/yt/client/api/public.h>

#include <util/generic/string.h>

namespace NYql {

// Settings for a QYT topic client backed by a YTsaurus queue.
//
// The client emulates the message stream read session (push) model on top of
// the YTsaurus queue (pull) API:
//   * read  session  -> pull_queue_consumer polling loop
//   * CommitOffset    -> advance_queue_consumer (with CAS on the previous offset)
struct TQytMessageStreamClientSettings {
    // Authenticated client for the YT cluster that hosts the queue.
    NYT::NApi::IClientPtr Client;

    // Optional path prefix (YT directory) that is prepended to relative topic paths.
    TString PathPrefix;

    // Name of the column that carries the message payload inside a queue row.
    TString DataColumn = "data";

    // Batch limits for a single pull_queue_consumer request.
    i64 MaxRowCount = 1000;
    i64 MaxDataWeight = 16 << 20; // 16 MB

    // Poll period used when the queue has no new rows to return.
    ui64 PollPeriodMs = 50;
};

std::shared_ptr<NFq::IMessageStreamClient> CreateQytMessageStreamClient(const TString& stream, const TQytMessageStreamClientSettings& settings);

} // namespace NYql
