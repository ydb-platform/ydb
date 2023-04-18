#pragma once

#include <util/generic/buffer.h>
#include <util/string/cast.h>
#include <util/string/printf.h>

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

    TKeyPrefix(EType type, const ui32 partition)
        : Partition(partition)
    {
        Resize(UnmarkedSize());
        *PtrType() = type;
        memcpy(PtrPartition(),  Sprintf("%.10" PRIu32, partition).data(), 10);
    }

    TKeyPrefix(EType type, const ui32 partition, EMark mark)
        : TKeyPrefix(type, partition)
    {
        Resize(MarkedSize());
        *PtrMark() = mark;
    }

    TKeyPrefix()
        : TKeyPrefix(TypeNone, 0)
    {}

    virtual ~TKeyPrefix()
    {}

    bool Marked(EMark mark) {
        if (Size() >= MarkedSize())
            return *PtrMark() == mark;
        return false;
    }

    static constexpr ui32 MarkPosition() { return UnmarkedSize(); }
    static constexpr ui32 MarkedSize() { return UnmarkedSize() + 1; }

    void SetType(EType type) {
        *PtrType() = type;
    }

    EType GetType() const {
        return EType(*PtrType());
    }

    ui32 GetPartition() const { return Partition; }

protected:
    static constexpr ui32 UnmarkedSize() { return 1 + 10; }

    void ParsePartition()
    {
        Partition = FromString<ui32>(TStringBuf{PtrPartition(), 10});
    }
private:
    char* PtrType() { return Data(); }
    char* PtrMark() { return Data() + UnmarkedSize(); }
    char* PtrPartition() { return Data() + 1; }

    const char* PtrType() const { return Data(); }
    const char* PtrMark() const { return Data() + UnmarkedSize(); }
    const char* PtrPartition() const { return Data() + 1; }

    ui32 Partition;
};

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
    TKey(EType type, const ui32 partition, const ui64 offset, const ui16 partNo, const ui32 count, const ui16 internalPartsCount, const bool isHead = false)
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
        Y_VERIFY(data.size() == KeySize() + IsHead());
        Y_VERIFY(*(PtrOffset() - 1) == '_');
        Y_VERIFY(*(PtrCount() - 1) == '_');
        Y_VERIFY(*(PtrPartNo() - 1) == '_');
        Y_VERIFY(*(PtrInternalPartsCount() - 1) == '_');

        ParsePartition();
        ParseOffset();
        ParseCount();
        ParsePartNo();
        ParseInternalPartsCount();
    }

    TKey()
        : TKey(TypeNone, 0, 0, 0, 0, 0)
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
        Y_VERIFY(Size() == KeySize() + IsHead());
        Offset = offset;
        memcpy(PtrOffset(), Sprintf("%.20" PRIu64, offset).data(), 20);
    }

    ui64 GetOffset() const {
        Y_VERIFY(Size() == KeySize() + IsHead());
        return Offset;
    }

    void SetCount(const ui32 count) {
        Y_VERIFY(Size() == KeySize() + IsHead());
        Count = count;
        memcpy(PtrCount(), Sprintf("%.10" PRIu32, count).data(), 10);
    }

    ui32 GetCount() const {
        Y_VERIFY(Size() == KeySize() + IsHead());
        return Count;
    }

    void SetPartNo(const ui16 partNo) {
        Y_VERIFY(Size() == KeySize() + IsHead());
        PartNo = partNo;
        memcpy(PtrPartNo(), Sprintf("%.5" PRIu16, partNo).data(), 5);
    }

    ui16 GetPartNo() const {
        Y_VERIFY(Size() == KeySize() + IsHead());
        return PartNo;
    }

    void SetInternalPartsCount(const ui16 internalPartsCount) {
        Y_VERIFY(Size() == KeySize() + IsHead());
        InternalPartsCount = internalPartsCount;
        memcpy(PtrInternalPartsCount(), Sprintf("%.5" PRIu16, internalPartsCount).data(), 5);
    }

    ui16 GetInternalPartsCount() const {
        Y_VERIFY(Size() == KeySize() + IsHead());
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

inline
TString GetTxKey(ui64 txId)
{
    return Sprintf("tx_%" PRIu64, txId);
}

}// NPQ
}// NKikimr
