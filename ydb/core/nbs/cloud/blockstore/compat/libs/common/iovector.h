#pragma once

#include <ydb/core/nbs/cloud/blockstore/compat/public/api/protos/io.pb.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/sglist.h>

namespace NCloud::NBlockStore {

using NYdb::NBS::TBlockDataRef;
using NYdb::NBS::TResultOrError;
using NYdb::NBS::TSgList;

////////////////////////////////////////////////////////////////////////////////

struct TCopyStats
{
    ui32 TotalBlockCount = 0;
    ui32 VoidBlockCount = 0;
};

////////////////////////////////////////////////////////////////////////////////

TSgList ResizeIOVector(NProto::TIOVector& iov, ui32 blockCount, ui32 blockSize);

TSgList GetSgList(const NProto::TIOVector& iov);
TSgList GetSgList(const NProto::TWriteBlocksRequest& request);

TResultOrError<TSgList> GetSgList(
    const NProto::TReadBlocksResponse& response,
    ui32 expectedBlockSize);

// Copy all data from iov to sglist. Skip first offsetInBlocks in sglist.
TCopyStats CopyToSgList(
    const NProto::TIOVector& src,
    const TSgList& dst,
    ui64 offsetInBlocks,
    ui32 blockSize);

// Copy all data from iov to sglist. Empty blocks from the srcData are
// transferred to the dstData as zeros, empty blocks in dstData are ignored.
size_t CopyToSgList(
    const NProto::TIOVector& srcData,
    const ui32 srcBlockSize,
    const TSgList& dstData,
    const ui32 dstBlockSize);

// Check all buffers, and trim those buffers that contain only zeros.
void TrimVoidBuffers(NProto::TIOVector& iov);

// Creates buffers for all blocks and copies only those that contain non-zeros.
// Buffers whose data is all zeros remain of zero size.
size_t CopyAndTrimVoidBuffers(
    TBlockDataRef src,
    ui32 blockCount,
    ui32 blockSize,
    NProto::TIOVector* dst);

// Count how many buffers are void.
[[nodiscard]] size_t CountVoidBuffers(const NProto::TIOVector& iov);

// Checks that the buffer contains only zeros.
[[nodiscard]] bool IsAllZeroes(const char* src, size_t size);

// Checks that the buffer contains only zeros.
[[nodiscard]] bool IsAllZeroes(TBlockDataRef dataRef);

// Checks that all buffers in the TIOVector contain only zeros.
[[nodiscard]] bool IsAllZeroes(const NProto::TIOVector& iov);

// Checks that all buffers in the TSgList contain only zeros.
[[nodiscard]] bool IsAllZeroes(const TSgList& sglist);

}   // namespace NCloud::NBlockStore
