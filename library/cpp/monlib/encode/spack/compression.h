#pragma once

#include "spack_v1.h"

#include <util/stream/input.h>
#include <util/stream/output.h>

namespace NMonitoring {

class IFramedCompressStream: public IOutputStream {
public:
    virtual void FlushWithoutEmptyFrame() = 0;
    virtual void FinishAndWriteEmptyFrame() = 0;
};

THolder<IInputStream> CompressedInput(IInputStream* in, ECompression alg);
THolder<IFramedCompressStream> CompressedOutput(IOutputStream* out, ECompression alg);

} // namespace NMonitoring
