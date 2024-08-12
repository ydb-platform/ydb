#pragma once
#include <ydb/core/formats/arrow/protos/ssa.pb.h>
#include <ydb/services/bg_tasks/abstract/interface.h>
#include <ydb/library/accessor/accessor.h>
#include <library/cpp/object_factory/object_factory.h>

namespace NKikimr::NOlap::NIndexes {

class IIndexChecker {
protected:
    virtual bool DoCheck(const THashMap<ui32, std::vector<TString>>& blobs) const = 0;
    virtual bool DoDeserializeFromProto(const NKikimrSSA::TProgram::TOlapIndexChecker& proto) = 0;
    virtual void DoSerializeToProto(NKikimrSSA::TProgram::TOlapIndexChecker& proto) const = 0;
    virtual std::set<ui32> DoGetIndexIds() const = 0;
public:
    using TFactory = NObjectFactory::TObjectFactory<IIndexChecker, TString>;
    using TProto = NKikimrSSA::TProgram::TOlapIndexChecker;
    virtual ~IIndexChecker() = default;
    bool Check(const THashMap<ui32, std::vector<TString>>& blobs) const {
        return DoCheck(blobs);
    }

    bool DeserializeFromProto(const NKikimrSSA::TProgram::TOlapIndexChecker& proto) {
        return DoDeserializeFromProto(proto);
    }

    void SerializeToProto(NKikimrSSA::TProgram::TOlapIndexChecker& proto) const {
        return DoSerializeToProto(proto);
    }

    std::set<ui32> GetIndexIds() const {
        return DoGetIndexIds();
    }

    virtual TString GetClassName() const = 0;
};

class TIndexCheckerContainer: public NBackgroundTasks::TInterfaceProtoContainer<IIndexChecker> {
private:
    using TBase = NBackgroundTasks::TInterfaceProtoContainer<IIndexChecker>;
public:
    TIndexCheckerContainer() = default;
    TIndexCheckerContainer(const std::shared_ptr<IIndexChecker>& object)
        : TBase(object) {
        AFL_VERIFY(Object);
    }
};

}   // namespace NKikimr::NOlap::NIndexes