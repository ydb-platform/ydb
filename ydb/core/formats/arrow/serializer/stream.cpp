#include "stream.h"
namespace NKikimr::NArrow {

arrow20::Status NSerialization::TFixedStringOutputStream::Write(const void* data, int64_t nbytes) {
    if (Y_LIKELY(nbytes > 0)) {
        Y_ABORT_UNLESS(Out && Out->size() - Position >= ui64(nbytes));
        char* dst = &(*Out)[Position];
        ::memcpy(dst, data, nbytes);
        Position += nbytes;
    }

    return arrow20::Status::OK();
}

arrow20::Status NSerialization::TFixedStringOutputStream::Close() {
    Out = nullptr;
    return arrow20::Status::OK();
}

}
