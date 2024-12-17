#include "scheme_types_defs.h"

#include <util/stream/output.h>


namespace NKikimr {
namespace NScheme {

namespace NNames {
    DECLARE_TYPED_TYPE_NAME(Int8);
    DECLARE_TYPED_TYPE_NAME(Uint8);
    DECLARE_TYPED_TYPE_NAME(Int16);
    DECLARE_TYPED_TYPE_NAME(Uint16);
    DECLARE_TYPED_TYPE_NAME(Int32);
    DECLARE_TYPED_TYPE_NAME(Uint32);
    DECLARE_TYPED_TYPE_NAME(Int64);
    DECLARE_TYPED_TYPE_NAME(Uint64);
    DECLARE_TYPED_TYPE_NAME(Bool);
    DECLARE_TYPED_TYPE_NAME(Double);
    DECLARE_TYPED_TYPE_NAME(Float);
    DECLARE_TYPED_TYPE_NAME(PairUi64Ui64);
    DECLARE_TYPED_TYPE_NAME(String);
    DECLARE_TYPED_TYPE_NAME(SmallBoundedString);
    DECLARE_TYPED_TYPE_NAME(LargeBoundedString);

    DECLARE_TYPED_TYPE_NAME(Utf8);
    DECLARE_TYPED_TYPE_NAME(Yson);
    DECLARE_TYPED_TYPE_NAME(Json);
    DECLARE_TYPED_TYPE_NAME(JsonDocument);

    DECLARE_TYPED_TYPE_NAME(Decimal);

    DECLARE_TYPED_TYPE_NAME(Date);
    DECLARE_TYPED_TYPE_NAME(Datetime);
    DECLARE_TYPED_TYPE_NAME(Timestamp);
    DECLARE_TYPED_TYPE_NAME(Interval);
    DECLARE_TYPED_TYPE_NAME(Date32);
    DECLARE_TYPED_TYPE_NAME(Datetime64);
    DECLARE_TYPED_TYPE_NAME(Timestamp64);
    DECLARE_TYPED_TYPE_NAME(Interval64);

    DECLARE_TYPED_TYPE_NAME(DyNumber);
    DECLARE_TYPED_TYPE_NAME(Uuid);
}

void WriteEscapedValue(IOutputStream &out, const char *data, size_t size) {
    static const size_t BUFFER_SIZE = 32;
    char buffer[BUFFER_SIZE];
    size_t bufPos = 0;

    for (const char *end = data + size; data != end; ++data) {
        if (bufPos >= BUFFER_SIZE - 1) {
            out.Write(buffer, bufPos);
            bufPos = 0;
        }
        const char c = *data;
        if (c == '#' || c == '/')
            buffer[bufPos++] = '\\';
        buffer[bufPos++] = c;
    }
    if (bufPos)
        out.Write(buffer, bufPos);
}

} // namespace NScheme
} // namespace NKikimr
