#pragma once

#include <ydb/public/sdk/cpp/src/client/topic/common/callback_context.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/codecs.h>
#include <ydb/public/sdk/cpp/src/client/topic/impl/common.h>
#include <ydb/public/sdk/cpp/src/client/topic/impl/read_session_impl.ipp>

namespace NYdb::inline Dev::NPersQueue {

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
