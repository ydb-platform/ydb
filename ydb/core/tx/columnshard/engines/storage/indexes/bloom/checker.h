#pragma once
#include <ydb/core/tx/columnshard/engines/scheme/indexes/abstract/simple.h>
namespace NKikimr::NOlap::NIndexes {

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