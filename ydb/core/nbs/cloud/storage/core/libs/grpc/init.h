#pragma once

#include "public.h"

class TLog;

namespace NYdb::NBS {

////////////////////////////////////////////////////////////////////////////////

// TGrpcInitializer is needed to call grpc_shutdown_blocking instead of
// grpc_shutdown see NBS-1032#5ea296701af0482eab4d6815
class TGrpcInitializer
{
public:
    TGrpcInitializer();
    ~TGrpcInitializer();
};

////////////////////////////////////////////////////////////////////////////////

// TLog is saved to the global variable, which is used by grpc logging callback.
// This means it also has to be stored somewhere else in case grpc lifetime is
// bound to some other global.
// Logger should only be initialized once. Some tests call `GrpcLoggerInit`
// several times but the logger will be initialized only once, other calls will
// be ignored.
void GrpcLoggerInit(TLog log, bool enableTracing);

}   // namespace NYdb::NBS
