#pragma once
#include <ydb/core/tx/columnshard/engines/scheme/indexes/abstract/simple.h>

#include <util/generic/bitmap.h>

namespace NKikimr::NOlap::NIndexes {

class TFixStringBitsStorage {
private:
    YDB_READONLY_DEF(TString, Data);

    template <class T>
    class TSizeDetector {};

    template <>
    class TSizeDetector<std::vector<bool>> {
    public:
        static ui32 GetSize(const std::vector<bool>& v) {
            return v.size();
        }
    };

    template <>
    class TSizeDetector<TDynBitMap> {
    public:
        static ui32 GetSize(const TDynBitMap& v) {
            return v.Size();
        }
    };

public:
    TFixStringBitsStorage(const TString& data)
        : Data(data) {
    }

    static ui32 GrowBitsCountToByte(const ui32 bitsCount) {
        const ui32 bytesCount = bitsCount / 8;
        return (bytesCount + ((bitsCount % 8) ? 1 : 0)) * 8;
    }

    TString DebugString() const {
        TStringBuilder sb;
        ui32 count1 = 0;
        ui32 count0 = 0;
        for (ui32 i = 0; i < GetSizeBits(); ++i) {
//            if (i % 20 == 0 && i) {
//                sb << i << " ";
//            }
            if (Get(i)) {
//                sb << 1 << " ";
                ++count1;
            } else {
//                sb << 0 << " ";
                ++count0;
            }
        }
        sb << GetSizeBits() << "=" << count0 << "[0]+" << count1 << "[1]";
        return sb;
    }

    template <class TBitsVector>
    TFixStringBitsStorage(const TBitsVector& bitsVector)
        : TFixStringBitsStorage(TSizeDetector<TBitsVector>::GetSize(bitsVector)) {
        ui32 byteIdx = 0;
        ui8 byteCurrent = 0;
        ui8 shiftCurrent = 1;
        for (ui32 i = 0; i < TSizeDetector<TBitsVector>::GetSize(bitsVector); ++i) {
            if (i && i % 8 == 0) {
                Data[byteIdx] = (char)byteCurrent;
                byteCurrent = 0;
                shiftCurrent = 1;
                ++byteIdx;
            }
            if (bitsVector[i]) {
                byteCurrent += shiftCurrent;
            }
            shiftCurrent = (shiftCurrent << 1);
        }
        if (byteCurrent) {
            Data[byteIdx] = (char)byteCurrent;
        }
    }

    ui32 GetSizeBits() const {
        return Data.size() * 8;
    }

    TFixStringBitsStorage(const ui32 sizeBits)
        : Data(GrowBitsCountToByte(sizeBits) / 8, '\0') {
    }

    void Set(const bool val, const ui32 idx) {
        AFL_VERIFY(idx < GetSizeBits());
        auto* start = &Data[idx / 8];
        ui8 word = (*(ui8*)start);
        if (val) {
            word |= 1 << (idx % 8);
        } else {
            word &= (Max<ui8>() - (1 << (idx % 8)));
        }
        memcpy(start, &word, sizeof(ui8));
    }

    bool Get(const ui32 idx) const {
        AFL_VERIFY(idx < GetSizeBits());
        const ui8 start = (*(ui8*)&Data[idx / 8]);
        return start & (1 << (idx % 8));
    }
};

class TBloomFilterChecker: public TSimpleIndexChecker {
public:
    static TString GetClassNameStatic() {
        return "BLOOM_FILTER";
    }

private:
    using TBase = TSimpleIndexChecker;
    std::set<ui64> HashValues;
    static inline auto Registrator = TFactory::TRegistrator<TBloomFilterChecker>(GetClassNameStatic());

protected:
    virtual bool DoDeserializeFromProtoImpl(const NKikimrSSA::TProgram::TOlapIndexChecker& proto) override;
    virtual void DoSerializeToProtoImpl(NKikimrSSA::TProgram::TOlapIndexChecker& proto) const override;

    virtual bool DoCheckImpl(const std::vector<TString>& blobs) const override;

public:
    TBloomFilterChecker() = default;
    TBloomFilterChecker(const ui32 indexId, std::set<ui64>&& hashes)
        : TBase(TIndexDataAddress(indexId))
        , HashValues(std::move(hashes)) {
    }
    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }
};

}   // namespace NKikimr::NOlap::NIndexes
