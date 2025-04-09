#pragma once

#include <ydb/core/kafka_proxy/actors/actors.h>

namespace NKafka {
    namespace NKafkaTransactions {
        class ResponseBuilder {
            public:
                template<class ResponseType, class RequestType>
                std::shared_ptr<ResponseType> Build(TMessagePtr<RequestType> request, EKafkaErrors errorCode);
        };
    } // namespace NKafkaTransactions
} // namespace NKafka