#pragma once
#include "common.h"

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/formats/arrow/protos/ssa.pb.h>
#include <ydb/services/bg_tasks/abstract/interface.h>

#include <library/cpp/object_factory/object_factory.h>
#include <util/digest/numeric.h>

namespace NKikimr::NOlap::NIndexes {

class TIndexDataAddress {
private:
    YDB_READONLY(ui32, IndexId, 0);
    YDB_READONLY_DEF(std::optional<ui64>, Category);

public:
    template <class TContainer>
    static std::set<ui32> ExtractIndexIds(const TContainer& addresses) {
        std::set<ui32> result;
        for (auto&& i : addresses) {
            result.emplace(i.GetIndexId());
        }
        return result;
    }

    TIndexDataAddress() = default;

    explicit TIndexDataAddress(const ui32 indexId)
        : IndexId(indexId) {
        AFL_VERIFY(IndexId);
    }

    explicit TIndexDataAddress(const ui32 indexId, const ui64 category)
        : IndexId(indexId)
        , Category(category) {
        AFL_VERIFY(IndexId);
    }

    bool operator<(const TIndexDataAddress& item) const {
        return std::tie(IndexId, Category) < std::tie(item.IndexId, item.Category);
    }

    bool operator==(const TIndexDataAddress& item) const {
        return std::tie(IndexId, Category) == std::tie(item.IndexId, item.Category);
    }

    operator size_t() const {
        if (Category) {
            return CombineHashes<ui64>(IndexId, *Category);
        } else {
            return IndexId;
        }
    }
};

class IIndexChecker {
protected:
    virtual bool DoCheck(const THashMap<TIndexDataAddress, std::vector<TString>>& blobs) const = 0;
    virtual bool DoDeserializeFromProto(const NKikimrSSA::TProgram::TOlapIndexChecker& proto) = 0;
    virtual void DoSerializeToProto(NKikimrSSA::TProgram::TOlapIndexChecker& proto) const = 0;
    virtual std::set<TIndexDataAddress> DoGetIndexIds() const = 0;

public:
    using TFactory = NObjectFactory::TObjectFactory<IIndexChecker, TString>;
    using TProto = NKikimrSSA::TProgram::TOlapIndexChecker;
    virtual ~IIndexChecker() = default;
    bool Check(const THashMap<TIndexDataAddress, std::vector<TString>>& blobs) const {
        return DoCheck(blobs);
    }

    bool DeserializeFromProto(const NKikimrSSA::TProgram::TOlapIndexChecker& proto) {
        return DoDeserializeFromProto(proto);
    }

    void SerializeToProto(NKikimrSSA::TProgram::TOlapIndexChecker& proto) const {
        return DoSerializeToProto(proto);
    }

    std::set<TIndexDataAddress> GetIndexIds() const {
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
