#pragma once

#include <ydb/public/sdk/cpp/client/ydb_topic/common/callback_context.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/codecs/codecs.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/impl/common.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/impl/read_session_impl.ipp>

namespace NYdb::NPersQueue {

// codecs
using NTopic::ICodec;
using NTopic::TCodecMap;
using NTopic::TGzipCodec;
using NTopic::TZstdCodec;
using NTopic::TUnsupportedCodec;

// callback_context
using NTopic::TCallbackContext;
using NTopic::TEnableSelfContext;
using NTopic::TContextOwner;

// common
// using NTopic::GetRetryErrorClass;
using NTopic::ISessionConnectionProcessorFactory;
using NTopic::CreateConnectionProcessorFactory;
using NTopic::TBaseSessionEventsQueue;
using NTopic::TWaiter;
using NTopic::IssuesSingleLineString;
using NTopic::MakeIssueWithSubIssues;
using NTopic::ApplyClusterEndpoint;
using NTopic::Cancel;
using NTopic::IsErrorMessage;
using NTopic::MakeErrorFromProto;

// counters_logger
using TCountersLogger = NTopic::TCountersLogger<true>;

// read_session_impl
using NTopic::HasNullCounters;
using NTopic::MakeCountersNotNull;
using TDeferredActions = NTopic::TDeferredActions<true>;
using TCallbackContextPtr = NTopic::TCallbackContextPtr<true>;
using TReadSessionEventsQueue = NTopic::TReadSessionEventsQueue<true>;
using TPartitionStreamImpl = NTopic::TPartitionStreamImpl<true>;
using TSingleClusterReadSessionImpl = NTopic::TSingleClusterReadSessionImpl<true>;

}  // namespace NYdb::NPersQueue
