#pragma once
#include "checker.h"

namespace NKikimr::NOlap::NIndexes {

class TSimpleIndexChecker: public IIndexChecker {
private:
    YDB_READONLY(ui32, IndexId, 0);
protected:
    virtual bool DoCheckImpl(const std::vector<TString>& blobs) const = 0;

    virtual bool DoCheck(const THashMap<ui32, std::vector<TString>>& blobs) const override final {
        auto it = blobs.find(IndexId);
        AFL_VERIFY(it != blobs.end());
        return DoCheckImpl(it->second);
    }
    virtual bool DoDeserializeFromProtoImpl(const NKikimrSSA::TProgram::TOlapIndexChecker& proto) = 0;
    virtual void DoSerializeToProtoImpl(NKikimrSSA::TProgram::TOlapIndexChecker& proto) const = 0;

    virtual bool DoDeserializeFromProto(const NKikimrSSA::TProgram::TOlapIndexChecker& proto) override final {
        IndexId = proto.GetIndexId();
        AFL_VERIFY(IndexId);
        return DoDeserializeFromProtoImpl(proto);
    }
    virtual void DoSerializeToProto(NKikimrSSA::TProgram::TOlapIndexChecker& proto) const override final {
        AFL_VERIFY(IndexId);
        proto.SetIndexId(IndexId);
        return DoSerializeToProtoImpl(proto);
    }
    virtual std::set<ui32> DoGetIndexIds() const override final {
        return {IndexId};
    }
public:
    TSimpleIndexChecker() = default;
    TSimpleIndexChecker(const ui32 indexId)
        : IndexId(indexId)
    {

    }
};

}   // namespace NKikimr::NOlap::NIndexes