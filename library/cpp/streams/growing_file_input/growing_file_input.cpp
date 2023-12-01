#include "growing_file_input.h"

#include <util/datetime/base.h>
#include <util/generic/yexception.h>

TGrowingFileInput::TGrowingFileInput(const TString& path)
    : File_(path, OpenExisting | RdOnly | Seq)
{
    if (!File_.IsOpen()) {
        ythrow TIoException() << "file " << path << " not open";
    }

    File_.Seek(0, sEnd);
}

TGrowingFileInput::TGrowingFileInput(const TFile& file)
    : File_(file)
{
    if (!File_.IsOpen()) {
        ythrow TIoException() << "file (" << file.GetName() << ") not open";
    }

    File_.Seek(0, sEnd);
}

size_t TGrowingFileInput::DoRead(void* buf, size_t len) {
    for (int sleepTime = 1;;) {
        size_t rr = File_.Read(buf, len);

        if (rr != 0) {
            return rr;
        }

        NanoSleep((ui64)sleepTime * 1000000);

        if (sleepTime < 2000) {
            sleepTime <<= 1;
        }
    }
}
