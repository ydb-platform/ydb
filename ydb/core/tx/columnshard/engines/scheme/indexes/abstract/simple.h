#pragma once
#include "checker.h"

namespace NKikimr::NOlap::NIndexes {

class TSimpleIndexChecker: public IIndexChecker {
private:
    YDB_READONLY_DEF(TIndexDataAddress, IndexId);

protected:
    virtual bool DoCheckImpl(const std::vector<TString>& blobs) const = 0;

    virtual bool DoCheck(const THashMap<TIndexDataAddress, std::vector<TString>>& blobs) const override final;
    virtual bool DoDeserializeFromProtoImpl(const NKikimrSSA::TProgram::TOlapIndexChecker& proto) = 0;
    virtual void DoSerializeToProtoImpl(NKikimrSSA::TProgram::TOlapIndexChecker& proto) const = 0;

    virtual bool DoDeserializeFromProto(const NKikimrSSA::TProgram::TOlapIndexChecker& proto) override final;
    virtual void DoSerializeToProto(NKikimrSSA::TProgram::TOlapIndexChecker& proto) const override final;
    virtual std::set<TIndexDataAddress> DoGetIndexIds() const override final {
        return { IndexId };
    }
public:
    TSimpleIndexChecker() = default;
    TSimpleIndexChecker(const TIndexDataAddress& indexId)
        : IndexId(indexId)
    {

    }
};

}   // namespace NKikimr::NOlap::NIndexes