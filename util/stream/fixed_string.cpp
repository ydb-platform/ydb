#include "fixed_string.h"

#include <util/string/builder.h>

TFixedStringStream::TFixedStringStream(const TString& data)
    : Data(data)
{
}

TFixedStringStream::TFixedStringStream(TString&& data)
    : Data(std::move(data))
{
}

void TFixedStringStream::MovePointer(size_t position) {
    Position = position;
}

size_t TFixedStringStream::DoRead(void* buf, size_t len) {
    len = std::min(len, Data.size() - Position);
    memcpy(buf, Data.data() + Position, len);
    Position += len;
    return len;
}

size_t TFixedStringStream::DoSkip(size_t len) {
    len = std::min(len, Data.size() - Position);
    Position += len;
    return len;
}

size_t TFixedStringStream::DoReadTo(TString& st, char ch) {
    size_t len = std::min(Data.find_first_of(ch, Position), Data.size()) - Position;
    st += Data.substr(Position, len);
    Position += len;
    return len;
}

ui64 TFixedStringStream::DoReadAll(IOutputStream& out) {
    out << Data.substr(Position);
    size_t len = Data.size() - Position;
    Position = Data.size();
    return len;
}
