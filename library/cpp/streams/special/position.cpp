#include "position.h"

TPositionStream::TPositionStream(TSimpleSharedPtr<IInputStream> slave)
    : Slave(slave)
    , Position(0)
{
}

size_t TPositionStream::DoRead(void* buf, size_t len) {
    size_t readed = Slave->Read(buf, len);
    Position += readed;
    return readed;
}

size_t TPositionStream::DoSkip(size_t len) {
    ui64 pos = Position;
    return SkipTo(Position + len) - pos;
}
