#pragma once

#include <util/generic/strbuf.h>
#include <util/generic/vector.h>
#include <util/system/unaligned_mem.h>

namespace NYql {
namespace NUdf {

class TInputBuffer {
public:
    TInputBuffer(TStringBuf buf)
        : Buf_(buf)
    {}

    char PopChar() {
        Ensure(1);
        char c = Buf_.Data()[Pos_];
        ++Pos_;
        return c;
    }

    template <typename T>
    T PopNumber() {
        Ensure(sizeof(T));
        T t = ReadUnaligned<T>(Buf_.Data() + Pos_);
        Pos_ += sizeof(T);
        return t;
    }

    std::string_view PopString() {
        ui32 size = PopNumber<ui32>();
        Ensure(size);
        std::string_view result(Buf_.Data() + Pos_, size);
        Pos_ += size;
        return result;
    }

private:
    void Ensure(size_t delta) {
        Y_ENSURE(Pos_ + delta <= Buf_.Size(), "Unexpected end of buffer");
    }

private:
    size_t Pos_ = 0;
    TStringBuf Buf_;
};

class TOutputBuffer {
public:
    void PushChar(char c) {
        Ensure(1);
        Vec_[Pos_] = c;
        ++Pos_;
    }

    template <typename T>
    void PushNumber(T t) {
        Ensure(sizeof(T));
        WriteUnaligned<T>(Vec_.data() + Pos_, t);
        Pos_ += sizeof(T);
    }

    void PushString(std::string_view data) {
        Ensure(sizeof(ui32) + data.size());
        *(ui32*)&Vec_[Pos_] = data.size();
        Pos_ += sizeof(ui32);
        std::memcpy(Vec_.data() + Pos_, data.data(), data.size());
        Pos_ += data.size();
    }

    // fill with zeros
    void Resize(size_t size) {
        Pos_ = 0;
        Vec_.clear();
        Vec_.resize(size);
    }

    void Rewind() {
        Pos_ = 0;
    }

    TStringBuf Finish() const {
        return TStringBuf(Vec_.data(), Vec_.data() + Pos_);
    }

private:
    void Ensure(size_t delta) {
        if (Pos_ + delta > Vec_.size()) {
            if (Pos_ + delta > Vec_.capacity()) {
                Vec_.reserve(Max(2 * Vec_.capacity(), Pos_ + delta));
            }
            // TODO: replace TVector - resize() performs unneeded zeroing here
            Vec_.resize(Pos_ + delta);
        }
    }

private:
    size_t Pos_ = 0;
    TVector<char> Vec_;
};



}
}
