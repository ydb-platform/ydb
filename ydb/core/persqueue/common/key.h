#pragma once

#include <ydb/core/persqueue/common/partition_id.h>
#include <ydb/core/persqueue/public/key.h>
#include <ydb/library/actors/core/log.h>

#include <util/digest/multi.h>
#include <util/generic/buffer.h>
#include <util/string/cast.h>
#include <util/string/printf.h>
#include <util/str_stl.h>

namespace NKikimr {
namespace NPQ {

// {char type; ui32 partition; (char mark)}
class TKeyPrefix : public TBuffer
{
public:
    enum EType : char {
        TypeNone = 0,
        TypeInfo = 'm',
        TypeData = 'd',
        TypeDeduplicator = 'e',
        TypeTmpData = 'x',
        TypeMeta = 'i',
        TypeTxMeta = 'I',
        TypeMLPConsumerData = 'c',
    };

    enum EMark : char {
        MarkUser = 'c',
        MarkProtoSourceId = 'p',
        MarkSourceId = 's',
        MarkUserDeprecated = 'u',
        MarkMLPSnapshot = 'S',
        MarkMLPWAL = 'w',
    };

    enum EServiceType : char {
        ServiceTypeInfo = 'M',
        ServiceTypeData = 'D',
        ServiceTypeTmpData = 'X',
        ServiceTypeMeta = 'J',
        ServiceTypeTxMeta = 'K'
    };

    TKeyPrefix(EType type, const TPartitionId& partition)
        : Partition(partition)
    {
        Resize(UnmarkedSize());
        SetTypeImpl(type, IsServicePartition());
        memcpy(PtrPartition(), Sprintf("%.10" PRIu32, Partition.InternalPartitionId).data(), 10);
    }

    TKeyPrefix(EType type, const TPartitionId& partition, EMark mark)
        : TKeyPrefix(type, partition)
    {
        Resize(MarkedSize());
        *PtrMark() = mark;
    }

    TKeyPrefix()
        : TKeyPrefix(TypeNone, TPartitionId(0))
    {}

    virtual ~TKeyPrefix()
    {}

    TString ToString() const {
        return {Data(), Size()};
    }

    bool Marked(EMark mark) {
        if (Size() >= MarkedSize())
            return *PtrMark() == mark;
        return false;
    }

    static constexpr ui32 MarkPosition() { return UnmarkedSize(); }
    static constexpr ui32 MarkedSize() { return UnmarkedSize() + 1; }


    void SetType(EType type) {
        SetTypeImpl(type, IsServicePartition() || HasServiceType());
    }

    EType GetType() const {
        switch (*PtrType()) {
            case TypeNone:
                return TypeNone;
            case TypeData:
            case ServiceTypeData:
                return TypeData;
            case TypeTmpData:
            case ServiceTypeTmpData:
                return TypeTmpData;
            case TypeInfo:
            case ServiceTypeInfo:
                return TypeInfo;
            case TypeMeta:
            case ServiceTypeMeta:
                return TypeMeta;
            case TypeTxMeta:
            case ServiceTypeTxMeta:
                return TypeTxMeta;
        }
        Y_ABORT();
        return TypeNone;
    }

    bool IsServicePartition() const {return Partition.WriteId.Defined();}

    const TPartitionId& GetPartition() const { return Partition; }

protected:
    static constexpr ui32 UnmarkedSize() { return 1 + 10; }

    void ParsePartition()
    {
        Partition.OriginalPartitionId = FromString<ui32>(TStringBuf{PtrPartition(), 10});
        Partition.InternalPartitionId = Partition.OriginalPartitionId;
    }

    bool HasServiceType() const;

private:

    void SetTypeImpl(EType, bool isServicePartition);

    char* PtrType() { return Data(); }
    char* PtrMark() { return Data() + UnmarkedSize(); }
    char* PtrPartition() { return Data() + 1; }

    const char* PtrType() const { return Data(); }
    const char* PtrMark() const { return Data() + UnmarkedSize(); }
    const char* PtrPartition() const { return Data() + 1; }

    TPartitionId Partition;
};

std::pair<TKeyPrefix, TKeyPrefix> MakeKeyPrefixRange(TKeyPrefix::EType type, const TPartitionId& partition);

// {char type; ui32 partition; ui64 offset; ui16 partNo; ui32 count, ui16 internalPartsCount}
// optional: _<offsetDelta 10 chars>
// optional suffix byte: Head ('|'), FastWrite ('?'), or legacy '\0'
// Serialized size is KeySize() or KeySizeWithOffsetDelta(), optionally +1 for suffix.
// offset, partNo - index of first rec
// count - diff of last record offset and first record offset in blob
// internalPartsCount - number of internal parts
// offsetDelta (ui32, 10 decimal digits, same as count) - optional extension; absent in legacy keys;
// means that all offsets in this blob are between offset and offset + offsetDelta
// A4|A5B1B2C1C2C3|D1 - Offset A, partNo 5, count 2, internalPartsCount 3
// ^    ^   ^ ^
// internalparts
class TKey : public TKeyPrefix
{
public:
    enum ESuffix : char {
        FastWrite = '?',
        Head = '|'
    };

    static TKey ForBody(EType type,
                        const TPartitionId& partition,
                        const ui64 offset,
                        const ui16 partNo,
                        const ui32 count,
                        const ui16 internalPartsCount,
                        const TMaybe<ui32>& offsetDelta = Nothing());
    static TKey ForHead(EType type,
                        const TPartitionId& partition,
                        const ui64 offset,
                        const ui16 partNo,
                        const ui32 count,
                        const ui16 internalPartsCount,
                        const TMaybe<ui32>& offsetDelta = Nothing());
    static TKey ForFastWrite(EType type,
                             const TPartitionId& partition,
                             const ui64 offset,
                             const ui16 partNo,
                             const ui32 count,
                             const ui16 internalPartsCount,
                             const TMaybe<ui32>& offsetDelta = Nothing());

    static TKey FromString(const TString& s) { return {s}; }
    static TKey FromString(const TString& s, const TPartitionId& partition);

    static TKey FromKey(const TKey& k,
                        EType type,
                        const TPartitionId& partitionId,
                        ui64 offset);

    TKey()
        : TKey(TypeNone, TPartitionId(0), 0, 0, 0, 0, false)
    {}

    TKey(const TKey& key)
        : TKey(key.GetType(), key.GetPartition(), key.Offset, key.PartNo, key.Count, key.InternalPartsCount, key.GetSuffix(), key.GetOffsetDelta())
    {
    }

    virtual ~TKey()
    {}

    void SetOffset(const ui64 offset) {
        EnsureValidBodySize();
        Offset = offset;
        memcpy(PtrOffset(), Sprintf("%.20" PRIu64, offset).data(), 20);
    }

    ui64 GetOffset() const {
        EnsureValidBodySize();
        return Offset;
    }

    void SetCount(const ui32 count) {
        EnsureValidBodySize();
        Count = count;
        memcpy(PtrCount(), Sprintf("%.10" PRIu32, count).data(), 10);
    }

    ui32 GetCount() const {
        EnsureValidBodySize();
        return Count;
    }

    void SetPartNo(const ui16 partNo) {
        EnsureValidBodySize();
        PartNo = partNo;
        memcpy(PtrPartNo(), Sprintf("%.5" PRIu16, partNo).data(), 5);
    }

    ui16 GetPartNo() const {
        EnsureValidBodySize();
        return PartNo;
    }

    void SetInternalPartsCount(const ui16 internalPartsCount) {
        EnsureValidBodySize();
        InternalPartsCount = internalPartsCount;
        memcpy(PtrInternalPartsCount(), Sprintf("%.5" PRIu16, internalPartsCount).data(), 5);
    }

    ui16 GetInternalPartsCount() const {
        EnsureValidBodySize();
        return InternalPartsCount;
    }

    void SetOffsetDelta(const TMaybe<ui32>& offsetDelta) {
        EnsureValidBodySize();
        OffsetDelta = offsetDelta;
        const TMaybe<char> suffix = GetSuffix();
        const ui32 bodySize = offsetDelta.Defined() ? KeySizeWithOffsetDelta() : KeySize();
        Resize(bodySize + suffix.Defined());
        if (offsetDelta.Defined()) {
            Data()[KeySize()] = '_';
            memcpy(PtrOffsetDelta(), Sprintf("%.10" PRIu32, *offsetDelta).data(), 10);
        }
        if (suffix.Defined()) {
            Data()[bodySize] = *suffix;
        }
    }

    void SetOffsetDelta(ui64 offsetDelta) {
        AFL_ENSURE(offsetDelta <= Max<ui32>());
        SetOffsetDelta(TMaybe<ui32>(static_cast<ui32>(offsetDelta)));
    }

    TMaybe<ui32> GetOffsetDelta() const {
        EnsureValidBodySize();
        return OffsetDelta;
    }

    bool HasOffsetDelta() const {
        return GetBodySize() == KeySizeWithOffsetDelta();
    }

    bool HasSuffix() const {
        return GetSuffix().Defined();
    }

    TMaybe<char> GetSuffix() const
    {
        const bool canHaveSuffix = Size() == KeySize() + 1 || Size() == KeySizeWithOffsetDelta() + 1;
        if (!canHaveSuffix) {
            return Nothing();
        }
        const char last = Data()[Size() - 1];
        if (last == ESuffix::Head || last == ESuffix::FastWrite) {
            return last;
        }
        return Nothing();
    }

    bool IsHead() const;
    bool IsFastWrite() const;

    static constexpr ui32 KeySize() {
        return UnmarkedSize() + 1 + 20 + 1 + 5 + 1 + 10 + 1 + 5;
        //p<partition 10 chars>_<offset 20 chars>_<part number 5 chars>_<count 10 chars>_<internalPartsCount count 5 chars>
    }

    static constexpr ui32 OffsetDeltaFieldSize() {
        return 1 + 10;
    }

    static constexpr ui32 KeySizeWithOffsetDelta() {
        return KeySize() + OffsetDeltaFieldSize();
    }

    static bool IsValidSerializedSize(const size_t size) {
        return size == KeySize()
            || size == KeySize() + 1
            || size == KeySizeWithOffsetDelta()
            || size == KeySizeWithOffsetDelta() + 1;
    }

    bool operator==(const TKey& key) const
    {
        return Size() == key.Size() && strncmp(Data(), key.Data(), Size()) == 0;
    }
    bool operator<(const TKey& key) const
    {
        if (GetPartition() < key.GetPartition())
            return true;

        if (GetPartition() == key.GetPartition()) {
            if (GetOffset() < key.GetOffset())
                return true;
            if (GetOffset() == key.GetOffset()) {
                if (GetPartNo() < key.GetPartNo())
                    return true;
            }
        }
        return false;
    }

    void SetFastWrite();
    void SetBody();

private:
    ui32 GetBodySize() const {
        return HasSuffix() || HasLegacyEmptySuffix() ? Size() - 1 : Size();
    }

    bool HasLegacyEmptySuffix() const {
        return Size() == KeySize() + 1 && Data()[Size() - 1] == '\0';
    }

    void EnsureValidBodySize() const {
        const ui32 bodySize = GetBodySize();
        AFL_ENSURE(bodySize == KeySize() || bodySize == KeySizeWithOffsetDelta());
    }

    TKey(EType type, const TPartitionId& partition, const ui64 offset, const ui16 partNo, const ui32 count, const ui16 internalPartsCount, const TMaybe<char> suffix, const TMaybe<ui32> offsetDelta = Nothing())
        : TKeyPrefix(type, partition)
        , Offset(offset)
        , Count(count)
        , PartNo(partNo)
        , InternalPartsCount(internalPartsCount)
        , OffsetDelta(offsetDelta)
    {
        Resize(KeySize());
        *(PtrOffset() - 1) = *(PtrCount() - 1) = *(PtrPartNo() - 1) = *(PtrInternalPartsCount() - 1) = '_';
        SetOffset(offset);
        SetPartNo(partNo);
        SetCount(count);
        SetInternalPartsCount(InternalPartsCount);
        SetOffsetDelta(offsetDelta);
        SetSuffix(suffix);
    }

    TKey(const TString& data)
    {
        Assign(data.data(), data.size());
        const ui32 bodySize = GetBodySize();
        AFL_ENSURE(bodySize == KeySize() || bodySize == KeySizeWithOffsetDelta());
        AFL_ENSURE(*(PtrOffset() - 1) == '_');
        AFL_ENSURE(*(PtrCount() - 1) == '_');
        AFL_ENSURE(*(PtrPartNo() - 1) == '_');
        AFL_ENSURE(*(PtrInternalPartsCount() - 1) == '_');

        ParsePartition();
        ParseOffset();
        ParseCount();
        ParsePartNo();
        ParseInternalPartsCount();

        if (bodySize == KeySizeWithOffsetDelta()) {
            AFL_ENSURE(Data()[KeySize()] == '_');
            OffsetDelta = FromString<ui32>(TStringBuf{PtrOffsetDelta(), 10});
        } else {
            OffsetDelta = Nothing();
        }
    }

    char* PtrOffset() { return Data() + UnmarkedSize() + 1; }
    char* PtrPartNo() { return PtrOffset() + 20 + 1; }
    char* PtrCount() { return PtrPartNo() + 5 + 1; }
    char* PtrInternalPartsCount() { return PtrCount() + 10 + 1; }
    char* PtrOffsetDelta() { return Data() + KeySize() + 1; }

    const char* PtrOffset() const { return Data() + UnmarkedSize() + 1; }
    const char* PtrPartNo() const { return PtrOffset() + 20 + 1; }
    const char* PtrCount() const { return PtrPartNo() + 5 + 1; }
    const char* PtrInternalPartsCount() const { return PtrCount() + 10 + 1; }
    const char* PtrOffsetDelta() const { return Data() + KeySize() + 1; }

    void ParseOffset()
    {
        Offset = FromString<ui64>(TStringBuf{PtrOffset(), 20});
    }

    void ParseCount()
    {
        Count = FromString<ui32>(TStringBuf{PtrCount(), 10});
    }

    void ParsePartNo()
    {
        PartNo = FromString<ui16>(TStringBuf{PtrPartNo(), 5});
    }

    void ParseInternalPartsCount()
    {
        InternalPartsCount = FromString<ui16>(TStringBuf{PtrInternalPartsCount(), 5});
    }

    void SetSuffix(TMaybe<char> suffix)
    {
        const ui32 bodySize = HasOffsetDelta() ? KeySizeWithOffsetDelta() : KeySize();
        Resize(bodySize + suffix.Defined());
        if (suffix.Defined()) {
            Data()[bodySize] = *suffix;
        }
    }

    ui64 Offset;
    ui32 Count;
    ui16 PartNo;
    ui16 InternalPartsCount;
    TMaybe<ui32> OffsetDelta;
};

inline
bool TKey::IsHead() const
{
    return GetSuffix() == ESuffix::Head;
}

inline
TString GetTxKey(ui64 txId, TMaybe<ui32> partition = Nothing())
{
    if (partition.Defined()) {
        return Sprintf("tx_%020" PRIu64 "_%010" PRIu32, txId, *partition);
    }
    return Sprintf("tx_%020" PRIu64, txId);
}

}// NPQ
}// NKikimr
