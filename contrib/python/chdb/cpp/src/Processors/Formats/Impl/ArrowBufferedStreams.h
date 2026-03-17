#pragma once
#include "clickhouse_config.h"

#if USE_ARROW || USE_ORC || USE_PARQUET

#include <optional>

#include <arrow/io/interfaces.h>
#include <arrow/memory_pool.h>

#define ORC_MAGIC_BYTES "ORC"
#define PARQUET_MAGIC_BYTES "PAR1"
#define ARROW_MAGIC_BYTES "ARROW1"

namespace DB_CHDB
{

class ReadBuffer;
class WriteBuffer;

class SeekableReadBuffer;
struct FormatSettings;

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
    RandomAccessFileFromSeekableReadBuffer(ReadBuffer & in_, std::optional<off_t> file_size_, bool avoid_buffering_);

    arrow20::Result<int64_t> GetSize() override;

    arrow20::Status Close() override;

    arrow20::Result<int64_t> Tell() const override;

    bool closed() const override { return !is_open; }

    arrow20::Result<int64_t> Read(int64_t nbytes, void * out) override;

    arrow20::Result<std::shared_ptr<arrow20::Buffer>> Read(int64_t nbytes) override;

    /// Override async reading to avoid using internal arrow thread pool.
    /// In our code we don't use async reading, so implementation is sync,
    /// we just call ReadAt and return future with ready value.
    arrow20::Future<std::shared_ptr<arrow20::Buffer>> ReadAsync(const arrow20::io::IOContext&, int64_t position, int64_t nbytes) override;

    arrow20::Status Seek(int64_t position) override;

private:
    ReadBuffer & in;
    SeekableReadBuffer & seekable_in;
    std::optional<off_t> file_size;
    bool is_open = false;
    bool avoid_buffering = false;

    ARROW_DISALLOW_COPY_AND_ASSIGN(RandomAccessFileFromSeekableReadBuffer);
};

class RandomAccessFileFromRandomAccessReadBuffer : public arrow20::io::RandomAccessFile
{
public:
    explicit RandomAccessFileFromRandomAccessReadBuffer(SeekableReadBuffer & in_, size_t file_size_);

    // These are thread safe.
    arrow20::Result<int64_t> GetSize() override;
    arrow20::Result<int64_t> ReadAt(int64_t position, int64_t nbytes, void* out) override;
    arrow20::Result<std::shared_ptr<arrow20::Buffer>> ReadAt(int64_t position, int64_t nbytes) override;
    arrow20::Future<std::shared_ptr<arrow20::Buffer>> ReadAsync(
        const arrow20::io::IOContext&, int64_t position, int64_t nbytes) override;

    // These are not thread safe, and arrow shouldn't call them. Return NotImplemented error.
    arrow20::Status Seek(int64_t) override;
    arrow20::Result<int64_t> Tell() const override;
    arrow20::Result<int64_t> Read(int64_t, void*) override;
    arrow20::Result<std::shared_ptr<arrow20::Buffer>> Read(int64_t) override;

    arrow20::Status Close() override;
    bool closed() const override { return !is_open; }

private:
    SeekableReadBuffer & in;
    size_t file_size;
    bool is_open = true;

    ARROW_DISALLOW_COPY_AND_ASSIGN(RandomAccessFileFromRandomAccessReadBuffer);
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

/// By default, arrow allocated memory using posix_memalign(), which is currently not equipped with
/// clickhouse memory tracking. This adapter adds memory tracking.
class ArrowMemoryPool : public arrow20::MemoryPool
{
public:
    static ArrowMemoryPool * instance();

    arrow20::Status Allocate(int64_t size, int64_t alignment, uint8_t ** out) override;
    arrow20::Status Reallocate(int64_t old_size, int64_t new_size, int64_t alignment, uint8_t ** ptr) override;
    void Free(uint8_t * buffer, int64_t size, int64_t alignment) override;

    std::string backend_name() const override { return "clickhouse"; }

    int64_t bytes_allocated() const override { return 0; }
    int64_t total_bytes_allocated() const override { return 0; }
    int64_t num_allocations() const override { return 0; }

private:
    ArrowMemoryPool() = default;
};

std::shared_ptr<arrow20::io::RandomAccessFile> asArrowFile(
    ReadBuffer & in,
    const FormatSettings & settings,
    std::atomic<int> & is_cancelled,
    const std::string & format_name,
    const std::string & magic_bytes,
    // If true, we'll use ReadBuffer::setReadUntilPosition() to avoid buffering and readahead as
    // much as possible. For HTTP or S3 ReadBuffer, this means that each RandomAccessFile
    // read call will do a new HTTP request. Used in parquet pre-buffered reading mode, which makes
    // arrow do its own buffering and coalescing of reads.
    // (ReadBuffer is not a good abstraction in this case, but it works.)
    bool avoid_buffering = false);

// Reads the whole file into a memory buffer, owned by the returned RandomAccessFile.
std::shared_ptr<arrow20::io::RandomAccessFile> asArrowFileLoadIntoMemory(
    ReadBuffer & in,
    std::atomic<int> & is_cancelled,
    const std::string & format_name,
    const std::string & magic_bytes);

}

#endif
