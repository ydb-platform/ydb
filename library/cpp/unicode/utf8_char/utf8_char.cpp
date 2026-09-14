#include "utf8_char.h"

#include <util/stream/output.h>

template <>
void Out<TUtf8Char>(IOutputStream& os, const TUtf8Char& obj) {
    os.Write(obj.data(), obj.length());
}
