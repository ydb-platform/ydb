#include "md5_stream.h"

namespace NYql {

TMd5OutputStream::TMd5OutputStream(IOutputStream& delegatee)
    : Delegatee_(delegatee)
{
}

TString TMd5OutputStream::Finalize() {
    char buf[33] = { 0 };
    return TString(Accumulator_.End(buf));
}

void TMd5OutputStream::DoWrite(const void* buf, size_t len) {
    Delegatee_.Write(buf, len);
    Accumulator_.Update(buf, len);
}

}
