#pragma once

#include <util/stream/buffered.h>
#include <util/stream/input.h>

class IZeroCopyInput;

/**
 * Unbuffered decompressing stream for .XZ and .LZMA files.
 *
 * Do not use it for reading in small pieces.
 */
class TUnbufferedXzDecompress: public IInputStream {
public:
    TUnbufferedXzDecompress(IInputStream* slave);
    TUnbufferedXzDecompress(IZeroCopyInput* slave);
    ~TUnbufferedXzDecompress() override;

private:
    size_t DoRead(void* buf, size_t len) override;

private:
    class TImpl;
    std::unique_ptr<TImpl> Impl_;
};

/**
 * Buffered decompressing stream for .XZ and .LZMA files.
 *
 * Supports efficient `ReadLine` calls and similar "reading in small pieces"
 * usage patterns.
 */
class TXzDecompress: public TBuffered<TUnbufferedXzDecompress> {
public:
    template <class T>
    inline TXzDecompress(T&& t, size_t buf = 1 << 13)
        : TBuffered<TUnbufferedXzDecompress>(buf, std::forward<T>(t))
    {
    }
};
