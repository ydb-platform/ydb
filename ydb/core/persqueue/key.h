#pragma once

#include "partition_id.h"

#include <util/digest/multi.h>
#include <util/generic/buffer.h>
#include <util/string/cast.h>
#include <util/string/printf.h>
#include <util/str_stl.h>

namespace NKikimr {
namespace NPQ {

// {char type; ui32 partiton; (char mark)}
class TKeyPrefix : public TBuffer
{
public:
    enum EType : char {
        TypeNone = 0,
        TypeInfo = 'm',
        TypeData = 'd',
        TypeTmpData = 'x',
        TypeMeta = 'i',
        TypeTxMeta = 'I'
    };

    enum EMark : char {
        MarkUser = 'c',
        MarkProtoSourceId = 'p',
        MarkSourceId = 's',
        MarkUserDeprecated = 'u'
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
        return TString(Data(), Size());
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
    enum EServiceType : char {
        ServiceTypeInfo = 'M',
        ServiceTypeData = 'D',
        ServiceTypeTmpData = 'X',
        ServiceTypeMeta = 'J',
        ServiceTypeTxMeta = 'K'
    };

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

// {char type; ui32 partiton; ui64 offset; ui16 partNo; ui32 count, ui16 internalPartsCount}
// offset, partNo - index of first rec
// count - diff of last record offset and first record offset in blob
// internalPartsCount - number of internal parts
// A4|A5B1B2C1C2C3|D1 - Offset A, partNo 5, count 2, internalPartsCount 3
// ^    ^   ^ ^
// internalparts
class TKey : public TKeyPrefix
{
public:
    TKey(EType type, const TPartitionId& partition, const ui64 offset, const ui16 partNo, const ui32 count, const ui16 internalPartsCount, const bool isHead = false)
        : TKeyPrefix(type, partition)
        , Offset(offset)
        , Count(count)
        , PartNo(partNo)
        , InternalPartsCount(internalPartsCount)
    {
        Resize(KeySize());
        *(PtrOffset() - 1) = *(PtrCount() - 1) = *(PtrPartNo() - 1) = *(PtrInternalPartsCount() - 1) = '_';
        SetOffset(offset);
        SetPartNo(partNo);
        SetCount(count);
        SetInternalPartsCount(InternalPartsCount);
        SetHead(isHead);
    }

    TKey(const TKey& key)
        : TKey(key.GetType(), key.GetPartition(), key.Offset, key.PartNo, key.Count, key.InternalPartsCount, key.IsHead())
    {
    }

    TKey(const TString& data)
    {
        Assign(data.data(), data.size());
        Y_ABORT_UNLESS(data.size() == KeySize() + IsHead());
        Y_ABORT_UNLESS(*(PtrOffset() - 1) == '_');
        Y_ABORT_UNLESS(*(PtrCount() - 1) == '_');
        Y_ABORT_UNLESS(*(PtrPartNo() - 1) == '_');
        Y_ABORT_UNLESS(*(PtrInternalPartsCount() - 1) == '_');

        ParsePartition();
        ParseOffset();
        ParseCount();
        ParsePartNo();
        ParseInternalPartsCount();
    }

    TKey()
        : TKey(TypeNone, TPartitionId(0), 0, 0, 0, 0)
    {}

    virtual ~TKey()
    {}

    TString ToString() const {
        return TString(Data(), Size());
    }

    void SetHead(const bool isHead) {
        Resize(KeySize() + isHead);
        if (isHead)
            Data()[KeySize()] = '|';
    }

    void SetOffset(const ui64 offset) {
        Y_ABORT_UNLESS(Size() == KeySize() + IsHead());
        Offset = offset;
        memcpy(PtrOffset(), Sprintf("%.20" PRIu64, offset).data(), 20);
    }

    ui64 GetOffset() const {
        Y_ABORT_UNLESS(Size() == KeySize() + IsHead());
        return Offset;
    }

    void SetCount(const ui32 count) {
        Y_ABORT_UNLESS(Size() == KeySize() + IsHead());
        Count = count;
        memcpy(PtrCount(), Sprintf("%.10" PRIu32, count).data(), 10);
    }

    ui32 GetCount() const {
        Y_ABORT_UNLESS(Size() == KeySize() + IsHead());
        return Count;
    }

    void SetPartNo(const ui16 partNo) {
        Y_ABORT_UNLESS(Size() == KeySize() + IsHead());
        PartNo = partNo;
        memcpy(PtrPartNo(), Sprintf("%.5" PRIu16, partNo).data(), 5);
    }

    ui16 GetPartNo() const {
        Y_ABORT_UNLESS(Size() == KeySize() + IsHead());
        return PartNo;
    }

    void SetInternalPartsCount(const ui16 internalPartsCount) {
        Y_ABORT_UNLESS(Size() == KeySize() + IsHead());
        InternalPartsCount = internalPartsCount;
        memcpy(PtrInternalPartsCount(), Sprintf("%.5" PRIu16, internalPartsCount).data(), 5);
    }

    ui16 GetInternalPartsCount() const {
        Y_ABORT_UNLESS(Size() == KeySize() + IsHead());
        return InternalPartsCount;
    }

    bool IsHead() const {
        return Size() == KeySize() + 1;
    }

    static constexpr ui32 KeySize() {
        return UnmarkedSize() + 1 + 20 + 1 + 5 + 1 + 10 + 1 + 5;
        //p<partition 10 chars>_<offset 20 chars>_<part number 5 chars>_<count 10 chars>_<internalPartsCount count 5 chars>
    }

    bool operator==(const TKey& key) const
    {
        return Size() == key.Size() && strncmp(Data(), key.Data(), Size()) == 0;
    }

private:
    char* PtrOffset() { return Data() + UnmarkedSize() + 1; }
    char* PtrPartNo() { return PtrOffset() + 20 + 1; }
    char* PtrCount() { return PtrPartNo() + 5 + 1; }
    char* PtrInternalPartsCount() { return PtrCount() + 10 + 1; }

    const char* PtrOffset() const { return Data() + UnmarkedSize() + 1; }
    const char* PtrPartNo() const { return PtrOffset() + 20 + 1; }
    const char* PtrCount() const { return PtrPartNo() + 5 + 1; }
    const char* PtrInternalPartsCount() const { return PtrCount() + 10 + 1; }

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

    ui64 Offset;
    ui32 Count;
    ui16 PartNo;
    ui16 InternalPartsCount;
};

TKey MakeKeyFromString(const TString& s, const TPartitionId& partition);

inline
TString GetTxKey(ui64 txId)
{
    return Sprintf("tx_%020" PRIu64, txId);
}


struct TReadSessionKey {
    TString SessionId;
    ui64 PartitionSessionId = 0;
    bool operator ==(const TReadSessionKey& rhs) const {
        return SessionId == rhs.SessionId && PartitionSessionId == rhs.PartitionSessionId;
    }
};

struct TDirectReadKey {
    TString SessionId;
    ui64 PartitionSessionId = 0;
    ui64 ReadId = 0;
    bool operator ==(const TDirectReadKey& rhs) const {
        return SessionId == rhs.SessionId && PartitionSessionId == rhs.PartitionSessionId && ReadId == rhs.ReadId;
    }
};

}// NPQ
}// NKikimr

template <>
struct THash<NKikimr::NPQ::TReadSessionKey> {
public:
    inline size_t operator()(const NKikimr::NPQ::TReadSessionKey& key) const {
        size_t res = 0;
        res += THash<TString>()(key.SessionId);
        res += THash<ui64>()(key.PartitionSessionId);
        return res;
    }
};
