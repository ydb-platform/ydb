#pragma once
#include "checker.h"

namespace NKikimr::NOlap::NIndexes {

class TCompositeIndexChecker: public IIndexChecker {
protected:
    std::vector<TIndexCheckerContainer> Checkers;
protected:
    virtual bool DoDeserializeFromProto(const NKikimrSSA::TProgram::TOlapIndexChecker& proto) override {
        for (auto&& i : proto.GetComposite().GetChildrenCheckers()) {
            TIndexCheckerContainer container;
            AFL_VERIFY(container.DeserializeFromProto(i));
            Checkers.emplace_back(std::move(container));
        }
        return true;
    }
    virtual void DoSerializeToProto(NKikimrSSA::TProgram::TOlapIndexChecker& proto) const override {
        for (auto&& i : Checkers) {
            i.SerializeToProto(*proto.MutableComposite()->AddChildrenCheckers());
        }
    }
    virtual std::set<ui32> DoGetIndexIds() const override {
        std::set<ui32> result;
        for (auto&& i : Checkers) {
            auto ids = i->GetIndexIds();
            result.insert(ids.begin(), ids.end());
        }
        return result;
    }
public:
    TCompositeIndexChecker() = default;
    TCompositeIndexChecker(const std::vector<TIndexCheckerContainer>& checkers)
        : Checkers(checkers) {

    }
    TCompositeIndexChecker(const std::vector<std::shared_ptr<IIndexChecker>>& checkers) {
        for (auto&& i : checkers) {
            Checkers.emplace_back(i);
        }
    }
};

class TAndIndexChecker: public TCompositeIndexChecker {
private:
    using TBase = TCompositeIndexChecker;
public:
    static TString GetClassNameStatic() {
        return "AND_FILTERS";
    }
private:
    static inline auto Registrator = TFactory::TRegistrator<TAndIndexChecker>(GetClassNameStatic());
protected:
    virtual bool DoCheck(const THashMap<ui32, std::vector<TString>>& blobsByIndexId) const override {
        for (auto&& i : Checkers) {
            if (!i->Check(blobsByIndexId)) {
                return false;
            }
        }
        return true;
    }
public:
    using TBase::TBase;

    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }
};

class TOrIndexChecker: public TCompositeIndexChecker {
private:
    using TBase = TCompositeIndexChecker;
public:
    static TString GetClassNameStatic() {
        return "OR_FILTERS";
    }
private:
    static inline auto Registrator = TFactory::TRegistrator<TOrIndexChecker>(GetClassNameStatic());
protected:
    virtual bool DoCheck(const THashMap<ui32, std::vector<TString>>& blobsByIndexId) const override {
//        ui32 idx = 0;
        for (auto&& i : Checkers) {
//            NActors::TLogContextGuard gLog = NActors::TLogContextBuilder::Build()("branch", idx++);
            if (i->Check(blobsByIndexId)) {
                return true;
            }
        }
        return false;
    }
public:
    using TBase::TBase;

    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }
};

}   // namespace NKikimr::NOlap::NIndexes