#include "stream.h"

#include <util/system/compiler.h>
#include <util/system/yassert.h>

namespace NYdb {

inline namespace Dev {

namespace NArrow {

arrow::Status NSerialization::TFixedStringOutputStream::Write(const void* data, int64_t nbytes) {
    if (Y_LIKELY(nbytes > 0)) {
        Y_ABORT_UNLESS(Out && Out->size() - Position >= uint64_t(nbytes));
        char* dst = &(*Out)[Position];
        ::memcpy(dst, data, nbytes);
        Position += nbytes;
    }

    return arrow::Status::OK();
}

arrow::Status NSerialization::TFixedStringOutputStream::Close() {
    Out = nullptr;
    return arrow::Status::OK();
}

}
}
}
