#include "console.h"

IOutputStream& PrintToConsoleStream(IOutputStream& stream, const TStringBuf& data) {
#ifdef _win_
    TStringBuf current = data;
    while (current.size() > 0) {
        size_t chunkSize = Min<size_t>(current.Size(), 0xFFFF);
        TStringBuf toWrite(current.data(), current.data() + chunkSize);
        stream << toWrite;
        current = TStringBuf(current.data() + chunkSize, current.data() + current.size());
    }
#else
    stream << data;
#endif
    return stream;
}
