#pragma once

#include "public.h"

#include <yt/yt/core/actions/public.h>

#include <util/stream/file.h>

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

struct TAppendableCompressedFileOptions
{
    bool WriteTruncateMessage = false;
    bool TryNotBreakLines = true;
};

IStreamLogOutputPtr CreateAppendableCompressedFile(
    TFile file,
    ILogCodecPtr codec,
    IInvokerPtr compressInvoker,
    const TAppendableCompressedFileOptions& options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
