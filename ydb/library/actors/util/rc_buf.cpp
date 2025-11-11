#include "rc_buf.h"

template<>
void Out<TRcBuf>(IOutputStream& s, const TRcBuf& x) {
    s.Write(TStringBuf(x));
}
