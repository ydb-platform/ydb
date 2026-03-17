#pragma once

#include <IO/ReadBuffer.h>
#include <IO/BufferWithOwnMemory.h>

#include <CHDBPoco/Net/HTTPBasicStreamBuf.h>


namespace DB_CHDB
{

class ReadBufferFromIStream : public BufferWithOwnMemory<ReadBuffer>
{
private:
    std::istream & istr;
    CHDBPoco::Net::HTTPBasicStreamBuf & stream_buf;
    bool eof = false;

    bool nextImpl() override;

public:
    explicit ReadBufferFromIStream(std::istream & istr_, size_t size = DBMS_DEFAULT_BUFFER_SIZE);
};

}
