#include "stream.h"

#include <util/generic/yexception.h>

namespace NKikimr::NArrow::NSerialization {

TStringInputStream::TStringInputStream(const TString& str) : Str_(str) {}

arrow::Result<int64_t> TStringInputStream::Read(int64_t nbytes, void* out) {
    Y_ENSURE(nbytes >= 0); // TODO: replace with returning proper arrow::Result.
    nbytes = std::min<int64_t>(nbytes, Str_.Size() - Pos_);

    memcpy(out, Str_.data() + Pos_, nbytes);
    Pos_ += nbytes;

    return nbytes;
}

arrow::Result<std::shared_ptr<arrow::Buffer>> TStringInputStream::Read(int64_t nbytes) {
    Y_ENSURE(nbytes >= 0); // TODO: replace with returning proper arrow::Result.
    nbytes = std::min<int64_t>(nbytes, Str_.Size() - Pos_);

    ARROW_ASSIGN_OR_RAISE(auto buffer, arrow::AllocateBuffer(nbytes));
    memcpy(buffer->mutable_data(), Str_.data() + Pos_, nbytes);
    Pos_ += nbytes;

    return buffer;
}

arrow::Status TStringInputStream::Close() {
    Closed_ = true;
    return arrow::Status::OK();
}

arrow::Result<int64_t> TStringInputStream::Tell() const {
    return Pos_;
}

bool TStringInputStream::closed() const {
    return Closed_;
}

arrow::Status TFixedStringOutputStream::Write(const void* data, int64_t nbytes) {
    if (Y_LIKELY(nbytes > 0)) {
        Y_ABORT_UNLESS(Out && Out->size() - Position >= ui64(nbytes));
        char* dst = &(*Out)[Position];
        ::memcpy(dst, data, nbytes);
        Position += nbytes;
    }
    return arrow::Status::OK();
}

arrow::Status TFixedStringOutputStream::Close() {
    Out = nullptr;
    return arrow::Status::OK();
}

}
