#pragma once

#include <ydb-cpp-sdk/client/topic/codecs.h>
#include <ydb-cpp-sdk/client/topic/counters.h>
#include <ydb-cpp-sdk/client/topic/errors.h>
#include <ydb-cpp-sdk/client/topic/events_common.h>
#include <ydb-cpp-sdk/client/topic/executor.h>
#include <ydb-cpp-sdk/client/topic/retry_policy.h>

namespace NYdb::inline Dev::NPersQueue {

// codecs
using NTopic::ECodec;
using NTopic::ICodec;
using NTopic::TCodecMap;
using NTopic::TGzipCodec;
using NTopic::TZstdCodec;
using NTopic::TUnsupportedCodec;

// counters
using NTopic::TCounterPtr;
using NTopic::TReaderCounters;
using NTopic::TWriterCounters;
using NTopic::MakeCountersNotNull;
using NTopic::HasNullCounters;

// errors
// using NTopic::GetRetryErrorClass;
// using NTopic::GetRetryErrorClassV2;

// common events
using NTopic::TWriteSessionMeta;
using NTopic::TMessageMeta;
using NTopic::TSessionClosedEvent;
using NTopic::TSessionClosedHandler;
// TODO reuse TPrintable

// executor
using NTopic::IExecutor;
using NTopic::CreateThreadPoolExecutorAdapter;
using NTopic::CreateThreadPoolExecutor;
using NTopic::CreateSyncExecutor;

// retry policy
using NTopic::IRetryPolicy;

}  // namespace NYdb::NPersQueue
