#pragma once
//#if !defined(ARCADIA_BUILD)
//#    include "config_formats.h"
//#endif

#if USE_ARROW || USE_ORC || USE_PARQUET

#include <arrow/io/interfaces.h>

namespace NDB
{

class ReadBuffer;
class SeekableReadBuffer;
class WriteBuffer;

class ArrowBufferedOutputStream : public arrow20::io::OutputStream
{
public:
    explicit ArrowBufferedOutputStream(WriteBuffer & out_);

    // FileInterface
    arrow20::Status Close() override;

    arrow20::Result<int64_t> Tell() const override;

    bool closed() const override { return !is_open; }

    // Writable
    arrow20::Status Write(const void * data, int64_t length) override;

private:
    WriteBuffer & out;
    int64_t total_length = 0;
    bool is_open = false;

    ARROW_DISALLOW_COPY_AND_ASSIGN(ArrowBufferedOutputStream);
};

class RandomAccessFileFromSeekableReadBuffer : public arrow20::io::RandomAccessFile
{
public:
    RandomAccessFileFromSeekableReadBuffer(SeekableReadBuffer & in_, off_t file_size_);

    arrow20::Result<int64_t> GetSize() override;

    arrow20::Status Close() override;

    arrow20::Result<int64_t> Tell() const override;

    bool closed() const override { return !is_open; }

    arrow20::Result<int64_t> Read(int64_t nbytes, void * out) override;

    arrow20::Result<std::shared_ptr<arrow20::Buffer>> Read(int64_t nbytes) override;

    arrow20::Status Seek(int64_t position) override;

private:
    SeekableReadBuffer & in;
    off_t file_size;
    bool is_open = false;

    ARROW_DISALLOW_COPY_AND_ASSIGN(RandomAccessFileFromSeekableReadBuffer);
};

class ArrowInputStreamFromReadBuffer : public arrow20::io::InputStream
{
public:
    explicit ArrowInputStreamFromReadBuffer(ReadBuffer & in);
    arrow20::Result<int64_t> Read(int64_t nbytes, void* out) override;
    arrow20::Result<std::shared_ptr<arrow20::Buffer>> Read(int64_t nbytes) override;
    arrow20::Status Abort() override;
    arrow20::Result<int64_t> Tell() const override;
    arrow20::Status Close() override;
    bool closed() const override { return !is_open; }

private:
    ReadBuffer & in;
    bool is_open = false;

    ARROW_DISALLOW_COPY_AND_ASSIGN(ArrowInputStreamFromReadBuffer);
};

std::shared_ptr<arrow20::io::RandomAccessFile> asArrowFile(ReadBuffer & in);

}

#endif
