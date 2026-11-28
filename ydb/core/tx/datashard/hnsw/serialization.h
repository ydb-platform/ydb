#pragma once

#include <util/system/types.h>
#include <istream>
#include <ostream>
#include <streambuf>

namespace NKikimr::NDataShard::NHnswSerialization {

// All snapshot integers (including float bit patterns) are little endian.
// Hash the stream incrementally so snapshots need not fit in a second buffer.
class TSnapshotStreamBuf : public std::streambuf {
public:
    explicit TSnapshotStreamBuf(std::streambuf* stream) : Stream(stream) {}
    ui64 Hash = 14695981039346656037ULL;

protected:
    std::streamsize xsputn(const char* data, std::streamsize size) override {
        const auto written = Stream->sputn(data, size);
        HashBytes(data, written);
        return written;
    }
    int_type overflow(int_type ch) override {
        if (traits_type::eq_int_type(ch, traits_type::eof()))
            return traits_type::not_eof(ch);
        const char byte = traits_type::to_char_type(ch);
        return xsputn(&byte, 1) == 1 ? ch : traits_type::eof();
    }
    std::streamsize xsgetn(char* data, std::streamsize size) override {
        const auto read = Stream->sgetn(data, size);
        HashBytes(data, read);
        return read;
    }
    int_type underflow() override { return Stream->sgetc(); }
    int_type uflow() override {
        auto ch = Stream->sbumpc();
        if (!traits_type::eq_int_type(ch, traits_type::eof())) {
            const char byte = traits_type::to_char_type(ch);
            HashBytes(&byte, 1);
        }
        return ch;
    }

private:
    void HashBytes(const char* data, std::streamsize size) {
        for (std::streamsize i = 0; i < size; ++i) {
            Hash ^= static_cast<unsigned char>(data[i]);
            Hash *= 1099511628211ULL;
        }
    }
    std::streambuf* Stream;
};


} // namespace NKikimr::NDataShard::NHnswSerialization
