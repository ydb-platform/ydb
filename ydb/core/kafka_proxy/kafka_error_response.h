#pragma once

#include <ydb/public/sdk/cpp/src/library/kafka/kafka.h>

namespace NKafka {

TApiMessage::TPtr BuildErrorResponse(const TApiMessage& request, EKafkaErrors error);

} // namespace NKafka
