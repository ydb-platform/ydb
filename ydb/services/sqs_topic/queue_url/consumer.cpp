#include "consumer.h"

namespace NKikimr::NSqsTopic::V1 {

    TString GetDefaultSqsConsumerName() {
        return "ydb-sqs-consumer";
    }
} // namespace NKikimr::NSqsTopic::V1
