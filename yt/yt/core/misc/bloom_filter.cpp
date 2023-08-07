#include "bloom_filter.h"
#include "blob.h"
#include "error.h"

#include <yt/yt_proto/yt/core/misc/proto/bloom_filter.pb.h>

#include <cmath>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TBloomFilterBase::TBloomFilterBase(int hashCount, int size)
    : HashCount_(hashCount)
    , LogSize_(Log2(size))
{ }

int TBloomFilterBase::GetVersion()
{
    return 1;
}

i64 TBloomFilterBase::Size() const
{
    return 1LL << LogSize_;
}

void TBloomFilterBase::VerifySize(ui64 size) const
{
    if (size != (1ULL << LogSize_)) {
        THROW_ERROR_EXCEPTION("Incorrect Bloom Filter size: expected a power of two, got %v",
            size);
    }
}

int TBloomFilterBase::BitPosition(int position) const
{
    return position & 7;
}

int TBloomFilterBase::BytePosition(int position) const
{
    return (position >> 3) & (Size() - 1);
}

int TBloomFilterBase::Log2(int size)
{
    int result = 0;
    while (size >> 1 != 0) {
        size >>= 1;
        ++result;
    }
    return result;
}

int TBloomFilterBase::HashCountFromRate(double falsePositiveRate)
{
    YT_VERIFY(falsePositiveRate > 0 && falsePositiveRate <= 1);
    int hashCount = std::log2(1.0 / falsePositiveRate);
    return hashCount > 0 ? hashCount : 1;
}

double TBloomFilterBase::BitsPerItemFromRate(double falsePositiveRate)
{
    YT_VERIFY(falsePositiveRate > 0 && falsePositiveRate <= 1);
    return 1.44 * std::log2(1.0 / falsePositiveRate);
}

////////////////////////////////////////////////////////////////////////////////

struct TBloomFilterBuilderTag { };

TBloomFilterBuilder::TBloomFilterBuilder(i64 capacity, double falsePositiveRate)
    : TBloomFilterBuilder(
        capacity,
        HashCountFromRate(falsePositiveRate),
        BitsPerItemFromRate(falsePositiveRate))
{ }

TBloomFilterBuilder::TBloomFilterBuilder(i64 capacity, int hashCount, double bitsPerItem)
    : TBloomFilterBuilder(
        TSharedMutableRef::Allocate<TBloomFilterBuilderTag>(1ULL << Log2(capacity * bitsPerItem / 8)),
        hashCount,
        bitsPerItem)
{ }

TBloomFilterBuilder::TBloomFilterBuilder(TSharedMutableRef data, int hashCount, double bitsPerItem)
    : TBloomFilterBase(hashCount, data.Size())
    , BitsPerItem_(bitsPerItem)
    , Data_(std::move(data))
{
    VerifySize(Data_.Size());
}

TSharedRef TBloomFilterBuilder::Bitmap() const
{
    return Data_.Slice(0, Size());
}

void TBloomFilterBuilder::Insert(TFingerprint fingerprint)
{
    YT_VERIFY(BitsPerItem_ > 0 && Size() > 0);

    int hash1 = static_cast<int>(fingerprint >> 32);
    int hash2 = static_cast<int>(fingerprint);

    for (int index = 0; index < HashCount_; ++index) {
        SetBit(hash1 + index * hash2);
    }

    ++InsertionCount_;
}

void TBloomFilterBuilder::SetBit(int position)
{
    Data_.Begin()[BytePosition(position)] |= (1 << BitPosition(position));
}

bool TBloomFilterBuilder::IsValid() const
{
    return !Data_.Empty() && InsertionCount_ * HashCount_ <= Size() * 8;
}

int TBloomFilterBuilder::EstimateLogSize() const
{
    i64 size = Size();
    int sizeLog = LogSize_;
    while ((size << 2) >= InsertionCount_ * BitsPerItem_) {
        size >>= 1;
        --sizeLog;
    }
    return sizeLog;
}

i64 TBloomFilterBuilder::EstimateSize() const
{
    return 1ULL << EstimateLogSize();
}

void TBloomFilterBuilder::Shrink()
{
    int sizeLog = EstimateLogSize();
    i64 size = 1LL << sizeLog;

    if (sizeLog < LogSize_) {
        auto sizeMask = size - 1;

        for (int index = size; index < Size(); ++index) {
            Data_.Begin()[index & sizeMask] |= Data_.Begin()[index];
        }

        LogSize_ = sizeLog;
    }
}

////////////////////////////////////////////////////////////////////////////////

TBloomFilter::TBloomFilter(TSharedRef data, int hashCount)
    : TBloomFilterBase(hashCount, data.Size())
    , Data_(std::move(data))
{
    VerifySize(Data_.Size());
}

TBloomFilter::TBloomFilter(TBloomFilter&& other)
{
    *this = std::move(other);
}

TBloomFilter& TBloomFilter::operator=(TBloomFilter&& other)
{
    HashCount_ = other.HashCount_;
    LogSize_ = other.LogSize_;
    Data_ = std::move(other.Data_);
    return *this;
}

bool TBloomFilter::Contains(TFingerprint fingerprint) const
{
    int hash1 = static_cast<int>(fingerprint >> 32);
    int hash2 = static_cast<int>(fingerprint);

    for (int index = 0; index < HashCount_; ++index) {
        if (!IsBitSet(hash1 + index * hash2)) {
            return false;
        }
    }

    return true;
}

bool TBloomFilter::IsBitSet(int position) const
{
    return Data_.Begin()[BytePosition(position)] & (1 << BitPosition(position));
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TBloomFilter* protoBloomFilter, const TBloomFilterBuilder& bloomFilter)
{
    protoBloomFilter->set_version(bloomFilter.GetVersion());
    protoBloomFilter->set_hash_count(bloomFilter.GetHashCount());
    protoBloomFilter->set_bitmap(ToString(bloomFilter.Bitmap()));
}

void FromProto(TBloomFilter* bloomFilter, const NProto::TBloomFilter& protoBloomFilter)
{
    if (protoBloomFilter.version() != TBloomFilter::GetVersion()) {
        THROW_ERROR_EXCEPTION("Bloom filter version mismatch: expected %v, got %v",
            TBloomFilter::GetVersion(),
            protoBloomFilter.version());
    }

    auto data = TSharedMutableRef::Allocate(protoBloomFilter.bitmap().size());
    ::memcpy(data.Begin(), protoBloomFilter.bitmap().data(), protoBloomFilter.bitmap().size());
    *bloomFilter = TBloomFilter(std::move(data), protoBloomFilter.hash_count());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

