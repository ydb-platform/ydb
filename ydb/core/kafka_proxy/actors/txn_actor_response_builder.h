#pragma once

#include <ydb/core/kafka_proxy/actors/actors.h>
#include <ydb/core/kafka_proxy/kafka.h>
#include <ydb/core/kafka_proxy/kafka_messages.h>

namespace NKafka::NKafkaTransactions {
    class TResponseBuilder {
        public:
            template<class ResponseType, class RequestType>
            std::shared_ptr<ResponseType> Build(TMessagePtr<RequestType> request, EKafkaErrors errorCode);
    };
} // namespace NKafka::NKafkaTransactions