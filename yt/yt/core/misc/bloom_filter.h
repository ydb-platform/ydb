#pragma once

#include "public.h"
#include "farm_hash.h"
#include "property.h"
#include "protobuf_helpers.h"

#include <library/cpp/yt/memory/ref.h>

#include <util/generic/noncopyable.h>

#include <util/system/defaults.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TBloomFilterBase
    : private TNonCopyable
{
public:
    TBloomFilterBase() = default;
    TBloomFilterBase(int hashCount, int size);

    static int GetVersion();

    i64 Size() const;

    DEFINE_BYVAL_RO_PROPERTY(int, HashCount);

protected:
    int LogSize_;

    void VerifySize(ui64 size) const;

    int BitPosition(int position) const;
    int BytePosition(int position) const;

    static int Log2(int size);
    static int HashCountFromRate(double falsePositiveRate);
    static double BitsPerItemFromRate(double falsePositiveRate);
};

class TBloomFilterBuilder
    : public TBloomFilterBase
{
public:
    TBloomFilterBuilder(i64 capacity, double falsePositiveRate);
    TBloomFilterBuilder(i64 capacity, int hashCount, double bitsPerItem);
    TBloomFilterBuilder(TSharedMutableRef data, int hashCount, double bitsPerItem);

    void Insert(TFingerprint fingerprint);

    i64 EstimateSize() const;

    bool IsValid() const;
    void Shrink();

    DEFINE_BYVAL_RO_PROPERTY(double, BitsPerItem);

protected:
    int InsertionCount_ = 0;
    TSharedMutableRef Data_;

    void SetBit(int position);
    int EstimateLogSize() const;
    TSharedRef Bitmap() const;

    friend void ToProto(NProto::TBloomFilter* protoBloomFilter, const TBloomFilterBuilder& bloomFilter);
};

class TBloomFilter
    : public TBloomFilterBase
{
public:
    TBloomFilter() = default;
    TBloomFilter(TBloomFilter&& other);
    TBloomFilter(TSharedRef data, int hashCount);

    TBloomFilter& operator=(TBloomFilter&& other);

    bool Contains(TFingerprint fingerprint) const;

protected:
    TSharedRef Data_;

    bool IsBitSet(int position) const;
};

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TBloomFilter* protoBloomFilter, const TBloomFilterBuilder& bloomFilter);
void FromProto(TBloomFilter* bloomFilter, const NProto::TBloomFilter& protoBloomFilter);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

