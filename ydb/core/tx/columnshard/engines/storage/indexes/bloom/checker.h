#pragma once
#include <ydb/core/tx/columnshard/engines/scheme/indexes/abstract/simple.h>
namespace NKikimr::NOlap::NIndexes {

class TFixStringBitsStorage {
private:
    YDB_READONLY_DEF(TString, Data);

public:
    TFixStringBitsStorage(const TString& data)
        : Data(data)
    {}

    ui32 GetSizeBits() const {
        return Data.size() * 8;
    }

    TFixStringBitsStorage(const ui32 sizeBits)
        : Data(sizeBits / 8 + ((sizeBits % 8) ? 1 : 0), '\0') {
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
        : TBase(indexId)
        , HashValues(std::move(hashes))
    {

    }
    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }
};

}   // namespace NKikimr::NOlap::NIndexes