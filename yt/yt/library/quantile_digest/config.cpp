#include "config.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void TTDigestConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("delta", &TThis::Delta)
        .Default(0.01);
    registrar.Parameter("compression_frequency", &TThis::CompressionFrequency)
        .Default(25);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
