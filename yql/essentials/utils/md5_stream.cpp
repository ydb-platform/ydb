#include "md5_stream.h"

#include <array>

namespace NYql {

TMd5OutputStream::TMd5OutputStream(IOutputStream& delegatee)
    : Delegatee_(delegatee)
{
}

TString TMd5OutputStream::Finalize() {
    std::array<char, 33> buf = {0};
    return TString(Accumulator_.End(buf.data()));
}

void TMd5OutputStream::DoWrite(const void* buf, size_t len) {
    Delegatee_.Write(buf, len);
    Accumulator_.Update(buf, len);
}

} // namespace NYql
