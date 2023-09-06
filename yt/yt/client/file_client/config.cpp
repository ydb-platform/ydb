#include "config.h"

namespace NYT::NFileClient {

////////////////////////////////////////////////////////////////////////////////

void TFileChunkWriterConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("block_size", &TThis::BlockSize)
        .Default(16_MB)
        .GreaterThan(0);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFileClient
