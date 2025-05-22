#pragma once

#include <util/generic/fwd.h>

namespace NKafka::NKafkaTransactionSql {

    constexpr ui32 PRODUCER_STATE_REQUEST_INDEX = 0;
    constexpr ui32 CONSUMER_STATES_REQUEST_INDEX = 1;

    extern const TString SELECT_FOR_VALIDATION;

} // namespace NKafka::NKafkaTransactionSql
