#pragma once

#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/public/sdk/cpp/src/library/kafka/kafka_log.h>

namespace NKafka {

inline NActors::NStructuredLog::TStructuredMessage LogPrefix() { return {}; }

}
