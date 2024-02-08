/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */


#include <aws/core/utils/crypto/CRC32.h>
#include <aws/core/utils/Outcome.h>
#include <aws/core/utils/crypto/Factories.h>
#include <aws/crt/Types.h>
#include <aws/checksums/crc.h>
#include <aws/common/byte_buf.h>

using namespace Aws::Utils::Crypto;

static Aws::Utils::ByteBuffer ByteBufferFromInt32(uint32_t value)
{
    Aws::Utils::ByteBuffer buffer(4);
    buffer[0] = (value >> 24) & 0xFF;
    buffer[1] = (value >> 16) & 0xFF;
    buffer[2] = (value >> 8) & 0xFF;
    buffer[3] = value & 0xFF;
    return buffer;
}

CRC32::CRC32() :
    m_hashImpl(CreateCRC32Implementation())
{
}

CRC32::~CRC32()
{
}

HashResult CRC32::Calculate(const Aws::String& str)
{
    return m_hashImpl->Calculate(str);
}

HashResult CRC32::Calculate(Aws::IStream& stream)
{
    return m_hashImpl->Calculate(stream);
}

void CRC32::Update(unsigned char* buffer, size_t bufferSize)
{
    m_hashImpl->Update(buffer, bufferSize);
}

HashResult CRC32::GetHash()
{
    return m_hashImpl->GetHash();
}

CRC32C::CRC32C() :
    m_hashImpl(CreateCRC32CImplementation())
{
}

CRC32C::~CRC32C()
{
}

HashResult CRC32C::Calculate(const Aws::String& str)
{
    return m_hashImpl->Calculate(str);
}

HashResult CRC32C::Calculate(Aws::IStream& stream)
{
    return m_hashImpl->Calculate(stream);
}


void CRC32C::Update(unsigned char* buffer, size_t bufferSize)
{
    m_hashImpl->Update(buffer, bufferSize);
}

HashResult CRC32C::GetHash()
{
    return m_hashImpl->GetHash();
}


CRC32Impl::CRC32Impl() : m_runningCrc32(0) {}

HashResult CRC32Impl::Calculate(const Aws::String& str)
{
    Aws::Crt::ByteCursor byteCursor = Aws::Crt::ByteCursorFromArray(reinterpret_cast<const uint8_t*>(str.data()), str.size());

    uint32_t runningCrc32 = 0;
    while (byteCursor.len > INT_MAX)
    {
        runningCrc32 = aws_checksums_crc32(byteCursor.ptr, INT_MAX, runningCrc32);
        aws_byte_cursor_advance(&byteCursor, INT_MAX);
    }
    runningCrc32 = aws_checksums_crc32(byteCursor.ptr, static_cast<int>(byteCursor.len), runningCrc32);
    const Aws::Utils::ByteBuffer& hash = ByteBufferFromInt32(runningCrc32);
    return HashResult(std::move(hash));
}

HashResult CRC32Impl::Calculate(Aws::IStream& stream)
{
    uint32_t runningCrc32 = 0;

    auto currentPos = stream.tellg();
    if (currentPos == std::ios::pos_type(-1))
    {
        currentPos = 0;
        stream.clear();
    }

    stream.seekg(0, stream.beg);

    uint8_t streamBuffer[Aws::Utils::Crypto::Hash::INTERNAL_HASH_STREAM_BUFFER_SIZE];
    while (stream.good())
    {
        stream.read(reinterpret_cast<char*>(streamBuffer), Aws::Utils::Crypto::Hash::INTERNAL_HASH_STREAM_BUFFER_SIZE);
        auto bytesRead = stream.gcount();

        if (bytesRead > 0)
        {
            runningCrc32 = aws_checksums_crc32(streamBuffer, static_cast<int>(bytesRead), runningCrc32);
        }
    }

    stream.clear();
    stream.seekg(currentPos, stream.beg);

    const Aws::Utils::ByteBuffer& hash = ByteBufferFromInt32(runningCrc32);
    return HashResult(std::move(hash));
}

void CRC32Impl::Update(unsigned char* buffer, size_t bufferSize)
{
    Aws::Crt::ByteCursor byteCursor = Aws::Crt::ByteCursorFromArray(buffer, bufferSize);

    while (byteCursor.len > INT_MAX)
    {
        m_runningCrc32 = aws_checksums_crc32(byteCursor.ptr, INT_MAX, m_runningCrc32);
        aws_byte_cursor_advance(&byteCursor, INT_MAX);
    }
    m_runningCrc32 = aws_checksums_crc32(byteCursor.ptr, static_cast<int>(byteCursor.len), m_runningCrc32);
}

HashResult CRC32Impl::GetHash()
{
    const Aws::Utils::ByteBuffer& hash = ByteBufferFromInt32(m_runningCrc32);
    return HashResult(std::move(hash));
}

CRC32CImpl::CRC32CImpl() : m_runningCrc32c(0) {}

HashResult CRC32CImpl::Calculate(const Aws::String& str)
{
    Aws::Crt::ByteCursor byteCursor = Aws::Crt::ByteCursorFromArray(reinterpret_cast<const uint8_t*>(str.data()), str.size());

    uint32_t runningCrc32c = 0;
    while (byteCursor.len > INT_MAX)
    {
        runningCrc32c = aws_checksums_crc32c(byteCursor.ptr, INT_MAX, runningCrc32c);
        aws_byte_cursor_advance(&byteCursor, INT_MAX);
    }
    runningCrc32c = aws_checksums_crc32c(byteCursor.ptr, static_cast<int>(byteCursor.len), runningCrc32c);
    const Aws::Utils::ByteBuffer& hash = ByteBufferFromInt32(runningCrc32c);
    return HashResult(std::move(hash));
}

HashResult CRC32CImpl::Calculate(Aws::IStream& stream)
{
    uint32_t runningCrc32c = 0;

    auto currentPos = stream.tellg();
    if (currentPos == std::ios::pos_type(-1))
    {
        currentPos = 0;
        stream.clear();
    }

    stream.seekg(0, stream.beg);

    uint8_t streamBuffer[Aws::Utils::Crypto::Hash::INTERNAL_HASH_STREAM_BUFFER_SIZE];
    while (stream.good())
    {
        stream.read(reinterpret_cast<char*>(streamBuffer), Aws::Utils::Crypto::Hash::INTERNAL_HASH_STREAM_BUFFER_SIZE);
        auto bytesRead = stream.gcount();

        if (bytesRead > 0)
        {
            runningCrc32c = aws_checksums_crc32c(streamBuffer, static_cast<int>(bytesRead), runningCrc32c);
        }
    }

    stream.clear();
    stream.seekg(currentPos, stream.beg);

    const Aws::Utils::ByteBuffer& hash = ByteBufferFromInt32(runningCrc32c);
    return HashResult(std::move(hash));
}

void CRC32CImpl::Update(unsigned char* buffer, size_t bufferSize)
{
    Aws::Crt::ByteCursor byteCursor = Aws::Crt::ByteCursorFromArray(buffer, bufferSize);

    while (byteCursor.len > INT_MAX)
    {
        m_runningCrc32c = aws_checksums_crc32c(byteCursor.ptr, INT_MAX, m_runningCrc32c);
        aws_byte_cursor_advance(&byteCursor, INT_MAX);
    }
    m_runningCrc32c = aws_checksums_crc32c(byteCursor.ptr, static_cast<int>(byteCursor.len), m_runningCrc32c);
}

HashResult CRC32CImpl::GetHash()
{
    const Aws::Utils::ByteBuffer& hash = ByteBufferFromInt32(m_runningCrc32c);
    return HashResult(std::move(hash));
}
