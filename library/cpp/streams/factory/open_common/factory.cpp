#include "factory.h"

#include <util/stream/file.h>
#include <util/system/file.h>
#include <util/generic/ptr.h>

#ifdef _win_ // isatty
#include <io.h>
#else
#include <unistd.h>
#endif

THolder<IInputStream> OpenStdin(size_t bufSize) {
    if (isatty(0)) {
        return MakeHolder<TUnbufferedFileInput>(Duplicate(0));
    }
    return MakeHolder<TFileInput>(Duplicate(0), bufSize);
}

