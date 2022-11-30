#pragma once

#include <util/generic/ptr.h>
#include <util/generic/yexception.h>
#include <util/stream/input.h>
#include <util/stream/output.h>

class TLzopCompress: public IOutputStream {
public:
    TLzopCompress(IOutputStream* slave, ui16 maxBlockSize = 1 << 15);
    ~TLzopCompress() override;

private:
    void DoWrite(const void* buf, size_t len) override;
    void DoFlush() override;
    void DoFinish() override;

private:
    class TImpl;
    THolder<TImpl> Impl_;
};

class TLzopDecompress: public IInputStream {
public:
    TLzopDecompress(IInputStream* slave, ui32 initialBufferSize = 1 << 16);
    ~TLzopDecompress() override;

private:
    size_t DoRead(void* buf, size_t len) override;

private:
    class TImpl;
    THolder<TImpl> Impl_;
};
