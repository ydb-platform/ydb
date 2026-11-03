#include "iovector.h"

#include <util/string/builder.h>

namespace NCloud::NBlockStore {

using NYdb::NBS::E_ARGUMENT;
using NYdb::NBS::MakeError;

////////////////////////////////////////////////////////////////////////////////

TSgList ResizeIOVector(NProto::TIOVector& iov, ui32 blockCount, ui32 blockSize)
{
    auto& buffers = *iov.MutableBuffers();
    buffers.Clear();
    buffers.Reserve(blockCount);

    TSgList sglist;
    sglist.reserve(blockCount);

    for (size_t i = 0; i < blockCount; ++i) {
        auto& buffer = *buffers.Add();
        buffer.ReserveAndResize(blockSize);

        sglist.emplace_back(buffer.data(), buffer.size());
    }

    return sglist;
}

TSgList GetSgList(const NProto::TIOVector& iov)
{
    TSgList sglist(Reserve(iov.BuffersSize()));

    for (const auto& buffer: iov.GetBuffers()) {
        if (buffer) {
            sglist.emplace_back(buffer.data(), buffer.size());
        } else {
            Y_DEBUG_ABORT_UNLESS(false, "write-request has empty buffer");
        }
    }

    return sglist;
}

TSgList GetSgList(const NProto::TWriteBlocksRequest& request)
{
    return GetSgList(request.GetBlocks());
}

TResultOrError<TSgList> GetSgList(
    const NProto::TReadBlocksResponse& response,
    ui32 expectedBlockSize)
{
    const auto& iov = response.GetBlocks();
    TSgList sglist(Reserve(iov.BuffersSize()));

    for (const auto& buffer: iov.GetBuffers()) {
        if (buffer) {
            if (buffer.size() != expectedBlockSize) {
                return MakeError(
                    E_ARGUMENT,
                    TStringBuilder() << "read-response has invalid buffer."
                                     << " BufferSize = " << buffer.size()
                                     << " BlockSize = " << expectedBlockSize);
            }
            sglist.emplace_back(buffer.data(), buffer.size());
        } else {
            sglist.emplace_back(
                TBlockDataRef::CreateZeroBlock(expectedBlockSize));
        }
    }

    return sglist;
}

TCopyStats CopyToSgList(
    const NProto::TIOVector& iov,
    const TSgList& sglist,
    ui64 offsetInBlocks,
    ui32 blockSize)
{
    TCopyStats result;

    ui64 offsetInBytes = offsetInBlocks * blockSize;

    // Skip offsetInBytes in destination
    size_t dstIndex = 0;
    for (; dstIndex != sglist.size(); ++dstIndex) {
        const size_t size = sglist[dstIndex].Size();

        Y_ABORT_UNLESS(size % blockSize == 0);

        if (offsetInBytes < size) {
            break;
        }
        offsetInBytes -= size;
    }

    for (const auto& src: iov.GetBuffers()) {
        Y_ABORT_UNLESS(src.size() == blockSize || src.size() == 0);
        Y_ABORT_UNLESS(dstIndex != sglist.size());

        TBlockDataRef dst = sglist[dstIndex];

        Y_ABORT_UNLESS(dst.Size() % blockSize == 0);
        Y_ABORT_UNLESS(dst.Size() - offsetInBytes >= blockSize);

        if (dst.Data()) {
            char* dstBuff = const_cast<char*>(dst.Data()) + offsetInBytes;
            if (src.empty()) {
                memset(dstBuff, 0, blockSize);
                ++result.VoidBlockCount;
            } else {
                memcpy(dstBuff, src.data(), blockSize);
            }
        }

        offsetInBytes += blockSize;

        if (offsetInBytes == dst.Size()) {
            ++dstIndex;
            offsetInBytes = 0;
        }
    }

    result.TotalBlockCount = iov.GetBuffers().size();
    return result;
}

size_t CopyToSgList(
    const NProto::TIOVector& srcData,
    const ui32 srcBlockSize,
    const TSgList& dstData,
    const ui32 dstBlockSize)
{
    size_t srcIndex = 0;
    size_t srcOffset = 0;
    size_t dstIndex = 0;
    size_t dstOffset = 0;
    size_t byteCount = 0;
    while (srcIndex < srcData.BuffersSize() && dstIndex < dstData.size()) {
        const auto& src = srcData.GetBuffers(srcIndex);
        TBlockDataRef dst = dstData[dstIndex];

        Y_ABORT_UNLESS(src.size() == srcBlockSize || src.size() == 0);
        Y_ABORT_UNLESS(dst.Size() == dstBlockSize);

        const size_t bytesToCopy =
            Min<size_t>(srcBlockSize - srcOffset, dst.Size() - dstOffset);

        if (dst.Data()) {
            char* dstBuff = const_cast<char*>(dst.Data()) + dstOffset;
            if (src.empty()) {
                memset(dstBuff, 0, bytesToCopy);
            } else {
                char* srcBuff = const_cast<char*>(src.data()) + srcOffset;
                memcpy(dstBuff, srcBuff, bytesToCopy);
            }
        }

        srcOffset += bytesToCopy;
        dstOffset += bytesToCopy;
        byteCount += bytesToCopy;

        if (srcOffset == srcBlockSize) {
            ++srcIndex;
            srcOffset = 0;
        }
        if (dstOffset == dstBlockSize) {
            ++dstIndex;
            dstOffset = 0;
        }
    }

    return byteCount;
}

void TrimVoidBuffers(NProto::TIOVector& iov)
{
    for (auto& buffer: *iov.MutableBuffers()) {
        if (IsAllZeroes(buffer.data(), buffer.size())) {
            buffer.clear();
        }
    }
}

size_t CopyAndTrimVoidBuffers(
    TBlockDataRef src,
    ui32 blockCount,
    ui32 blockSize,
    NProto::TIOVector* dst)
{
    Y_ABORT_UNLESS(static_cast<size_t>(blockCount) * blockSize == src.Size());

    size_t bytesCount = 0;

    auto& buffers = *dst->MutableBuffers();
    buffers.Clear();
    buffers.Reserve(blockCount);

    const char* srcBuffer = src.Data();
    for (size_t i = 0; i < blockCount; ++i) {
        if (IsAllZeroes(srcBuffer, blockSize)) {
            // Add an empty buffer when data contains all zeros.
            buffers.Add();
        } else {
            buffers.Add()->assign(srcBuffer, blockSize);
        }

        srcBuffer += blockSize;
        bytesCount += blockSize;
    }

    return bytesCount;
}

size_t CountVoidBuffers(const NProto::TIOVector& iov)
{
    size_t result = 0;
    for (const auto& buffer: iov.GetBuffers()) {
        if (buffer.empty()) {
            ++result;
        }
    }
    return result;
}

bool IsAllZeroes(const char* src, size_t size)
{
    using TBigNumber = ui64;

    if (size < sizeof(TBigNumber)) {
        for (size_t i = 0; i < size; ++i) {
            if (src[i] != 0) {
                return false;
            }
        }
        return true;
    }

    const bool isAligned =
        reinterpret_cast<std::uintptr_t>(src) % sizeof(TBigNumber) == 0;

    if (isAligned) {
        const TBigNumber* const a = reinterpret_cast<const TBigNumber*>(src);
        return !a[0] && !memcmp(
                            src,
                            src + sizeof(TBigNumber),
                            size - sizeof(TBigNumber));
    }
    return !src[0] && !memcmp(src, src + sizeof(char), size - sizeof(char));
}

bool IsAllZeroes(TBlockDataRef dataRef)
{
    return IsAllZeroes(dataRef.Data(), dataRef.Size());
}

bool IsAllZeroes(const NProto::TIOVector& iov)
{
    return AllOf(
        iov.GetBuffers(),
        [](const TString& buffer) {
            return buffer.empty() || IsAllZeroes(buffer.data(), buffer.size());
        });
}

bool IsAllZeroes(const TSgList& sglist)
{
    return AllOf(
        sglist,
        [](const TBlockDataRef& buffer)
        { return buffer.Empty() || IsAllZeroes(buffer); });
}

}   // namespace NCloud::NBlockStore
