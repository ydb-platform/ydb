#include "checksum.h"

namespace {

////////////////////////////////////////////////////////////////////////////////

extern "C" uint64_t crc64_yt_norm_base(
    uint64_t init_crc,        //!< initial CRC value, 64 bits
    const unsigned char *buf, //!< buffer to calculate CRC on
    uint64_t len              //!< buffer length in bytes (64-bit data)
);

uint64_t IsaCrcImplBase(const void* data, size_t length, uint64_t seed)
{
    seed = ~seed;

    uint64_t checksum = crc64_yt_norm_base(seed, reinterpret_cast<const unsigned char*>(data), length);

    return ~checksum;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace


namespace NYT::NIsaCrc64
{

////////////////////////////////////////////////////////////////////////////////

uint64_t CrcImplBase(const void* data, size_t length, uint64_t seed)
{
    return IsaCrcImplBase(data, length, seed);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIsaCrc64

#ifdef ISA_CRC64_FAST_IMPL_SUPPORTED

namespace
{

////////////////////////////////////////////////////////////////////////////////

extern "C" uint64_t crc64_yt_norm_by8(
    uint64_t init_crc,        //!< initial CRC value, 64 bits
    const unsigned char *buf, //!< buffer to calculate CRC on
    uint64_t len              //!< buffer length in bytes (64-bit data)
);

uint64_t IsaCrcImplFast(const void* data, size_t length, uint64_t seed)
{
    // These manipulations with seed are necessary in order to maintain compatibility.
    // Comparing YT's old implenetation to ISA's one, the latter performs `NOT` operations
    // before and after the function call, so we use `NOT` here to neutralize it.

    seed = ~seed;

    uint64_t checksum = crc64_yt_norm_by8(seed, reinterpret_cast<const unsigned char*>(data), length);

    return ~checksum;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

namespace NYT::NIsaCrc64
{

////////////////////////////////////////////////////////////////////////////////

uint64_t CrcImplFast(const void* data, size_t length, uint64_t seed)
{
    return IsaCrcImplFast(data, length, seed);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIsaCrc64

#endif // ISA_CRC64_FAST_IMPL_SUPPORTED
