#pragma once

#include <library/cpp/messagebus/test/example/common/messages.pb.h>

#include <library/cpp/messagebus/ybus.h>
#include <library/cpp/messagebus/protobuf/ybusbuf.h>

namespace NCalculator {
    typedef ::NBus::TBusBufferMessage<TRequestSumRecord, 1> TRequestSum;
    typedef ::NBus::TBusBufferMessage<TRequestMulRecord, 2> TRequestMul;
    typedef ::NBus::TBusBufferMessage<TResponseRecord, 3> TResponse;

    struct TCalculatorProtocol: public ::NBus::TBusBufferProtocol {
        TCalculatorProtocol();
    };

}
