#include "proto.h"

using namespace NCalculator;
using namespace NBus;

TCalculatorProtocol::TCalculatorProtocol()
    : TBusBufferProtocol("Calculator", 34567)
{
    RegisterType(new TRequestSum);
    RegisterType(new TRequestMul);
    RegisterType(new TResponse);
}
