#include <IO/NullWriteBuffer.h>


namespace DB_CHDB
{

NullWriteBuffer::NullWriteBuffer()
    : WriteBuffer(data, sizeof(data))
{
}

void NullWriteBuffer::nextImpl()
{
}

}
