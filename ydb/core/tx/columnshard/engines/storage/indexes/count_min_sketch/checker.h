#pragma once
#include <ydb/core/tx/columnshard/engines/scheme/indexes/abstract/simple.h>

namespace NKikimr::NOlap::NIndexes::NCountMinSketch {

class TCountMinSketchChecker: public TSimpleIndexChecker {
public:
    static TString GetClassNameStatic() {
        return "COUNT_MIN_SKETCH";
    }
private:
    using TBase = TSimpleIndexChecker;
    static inline auto Registrator = TFactory::TRegistrator<TCountMinSketchChecker>(GetClassNameStatic());

protected:
    virtual bool DoDeserializeFromProtoImpl(const NKikimrSSA::TProgram::TOlapIndexChecker& proto) override;
    virtual void DoSerializeToProtoImpl(NKikimrSSA::TProgram::TOlapIndexChecker& proto) const override;

    virtual bool DoCheckImpl(const std::vector<TString>& blobs) const override;

public:
    TCountMinSketchChecker() = default;
    TCountMinSketchChecker(const ui32 indexId)
        : TBase(indexId)
    {}

    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }
};

}   // namespace NKikimr::NOlap::NIndexes
