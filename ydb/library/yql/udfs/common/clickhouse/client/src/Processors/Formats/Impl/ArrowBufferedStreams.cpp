#include "ArrowBufferedStreams.h"

#if USE_ARROW || USE_ORC || USE_PARQUET

#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromString.h>
#include <IO/copyData.h>
#include <arrow/buffer.h>
#include <arrow/io/memory.h>
#include <arrow/result.h>

#include <sys/stat.h>


namespace NDB
{

ArrowBufferedOutputStream::ArrowBufferedOutputStream(WriteBuffer & out_) : out{out_}, is_open{true}
{
}

arrow20::Status ArrowBufferedOutputStream::Close()
{
    is_open = false;
    return arrow20::Status::OK();
}

arrow20::Result<int64_t> ArrowBufferedOutputStream::Tell() const
{
    return arrow20::Result<int64_t>(total_length);
}

arrow20::Status ArrowBufferedOutputStream::Write(const void * data, int64_t length)
{
    out.write(reinterpret_cast<const char *>(data), length);
    total_length += length;
    return arrow20::Status::OK();
}

RandomAccessFileFromSeekableReadBuffer::RandomAccessFileFromSeekableReadBuffer(SeekableReadBuffer & in_, off_t file_size_)
    : in{in_}, file_size{file_size_}, is_open{true}
{
}

arrow20::Result<int64_t> RandomAccessFileFromSeekableReadBuffer::GetSize()
{
    return arrow20::Result<int64_t>(file_size);
}

arrow20::Status RandomAccessFileFromSeekableReadBuffer::Close()
{
    is_open = false;
    return arrow20::Status::OK();
}

arrow20::Result<int64_t> RandomAccessFileFromSeekableReadBuffer::Tell() const
{
    return in.getPosition();
}

arrow20::Result<int64_t> RandomAccessFileFromSeekableReadBuffer::Read(int64_t nbytes, void * out)
{
    return in.readBig(reinterpret_cast<char *>(out), nbytes);
}

arrow20::Result<std::shared_ptr<arrow20::Buffer>> RandomAccessFileFromSeekableReadBuffer::Read(int64_t nbytes)
{
    ARROW_ASSIGN_OR_RAISE(auto buffer, arrow20::AllocateResizableBuffer(nbytes))
    ARROW_ASSIGN_OR_RAISE(int64_t bytes_read, Read(nbytes, buffer->mutable_data()))

    if (bytes_read < nbytes)
        RETURN_NOT_OK(buffer->Resize(bytes_read));

    return buffer;
}

arrow20::Status RandomAccessFileFromSeekableReadBuffer::Seek(int64_t position)
{
    in.seek(position, SEEK_SET);
    return arrow20::Status::OK();
}


ArrowInputStreamFromReadBuffer::ArrowInputStreamFromReadBuffer(ReadBuffer & in_) : in(in_), is_open{true}
{
}

arrow20::Result<int64_t> ArrowInputStreamFromReadBuffer::Read(int64_t nbytes, void * out)
{
    return in.readBig(reinterpret_cast<char *>(out), nbytes);
}

arrow20::Result<std::shared_ptr<arrow20::Buffer>> ArrowInputStreamFromReadBuffer::Read(int64_t nbytes)
{
    ARROW_ASSIGN_OR_RAISE(auto buffer, arrow20::AllocateResizableBuffer(nbytes))
    ARROW_ASSIGN_OR_RAISE(int64_t bytes_read, Read(nbytes, buffer->mutable_data()))

    if (bytes_read < nbytes)
        RETURN_NOT_OK(buffer->Resize(bytes_read));

    return buffer;
}

arrow20::Status ArrowInputStreamFromReadBuffer::Abort()
{
    return arrow20::Status();
}

arrow20::Result<int64_t> ArrowInputStreamFromReadBuffer::Tell() const
{
    return in.count();
}

arrow20::Status ArrowInputStreamFromReadBuffer::Close()
{
    is_open = false;
    return arrow20::Status();
}

std::shared_ptr<arrow20::io::RandomAccessFile> asArrowFile(ReadBuffer & in)
{
    if (auto * fd_in = dynamic_cast<ReadBufferFromFileDescriptor *>(&in))
    {
        struct stat stat;
        auto res = ::fstat(fd_in->getFD(), &stat);
        // if fd is a regular file i.e. not stdin
        if (res == 0 && S_ISREG(stat.st_mode))
            return std::make_shared<RandomAccessFileFromSeekableReadBuffer>(*fd_in, stat.st_size);
    }

    // fallback to loading the entire file in memory
    std::string file_data;
    {
        WriteBufferFromString file_buffer(file_data);
        copyData(in, file_buffer);
    }

    return std::make_shared<arrow20::io::BufferReader>(arrow20::Buffer::FromString(std::move(file_data)));
}

}

#endif
