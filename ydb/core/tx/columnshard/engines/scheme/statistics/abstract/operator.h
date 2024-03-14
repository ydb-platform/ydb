#pragma once
#include "common.h"
#include "portion_storage.h"

#include <ydb/core/tx/columnshard/engines/scheme/statistics/protos/data.pb.h>
#include <ydb/core/tx/columnshard/engines/scheme/abstract/index_info.h>

#include <ydb/services/bg_tasks/abstract/interface.h>
#include <ydb/library/accessor/accessor.h>

#include <library/cpp/object_factory/object_factory.h>

namespace NKikimr::NOlap {
class IPortionDataChunk;
}

namespace NKikimr::NOlap::NStatistics {

class IOperator {
private:
    YDB_READONLY(EType, Type, EType::Undefined);
    IOperator() = default;
protected:
    virtual void DoFillStatisticsData(const THashMap<ui32, std::vector<std::shared_ptr<IPortionDataChunk>>>& data, TPortionStorage& portionStats, const IIndexInfo& index) const = 0;
    virtual void DoShiftCursor(TPortionStorageCursor& cursor) const = 0;
    virtual bool DoDeserializeFromProto(const NKikimrColumnShardStatisticsProto::TOperatorContainer& proto) = 0;
    virtual void DoSerializeToProto(NKikimrColumnShardStatisticsProto::TOperatorContainer& proto) const = 0;
public:
    using TProto = NKikimrColumnShardStatisticsProto::TOperatorContainer;
    using TFactory = NObjectFactory::TObjectFactory<IOperator, TString>;

    virtual ~IOperator() = default;

    virtual std::vector<ui32> GetEntityIds() const = 0;

    IOperator(const EType type)
        :Type(type) {

    }

    void ShiftCursor(TPortionStorageCursor& cursor) const {
        DoShiftCursor(cursor);
    }

    void FillStatisticsData(const THashMap<ui32, std::vector<std::shared_ptr<IPortionDataChunk>>>& data, TPortionStorage& portionStats, const IIndexInfo& index) const {
        DoFillStatisticsData(data, portionStats, index);
    }

    TString GetClassName() const {
        return ::ToString(Type);
    }

    TIdentifier GetIdentifier() const {
        return TIdentifier(Type, GetEntityIds());
    }

    bool DeserializeFromProto(const NKikimrColumnShardStatisticsProto::TOperatorContainer& proto);

    void SerializeToProto(NKikimrColumnShardStatisticsProto::TOperatorContainer& proto) const {
        return DoSerializeToProto(proto);
    }
};

class TOperatorContainer: public NBackgroundTasks::TInterfaceProtoContainer<IOperator> {
private:
    std::optional<TPortionStorageCursor> Cursor;
    using TBase = NBackgroundTasks::TInterfaceProtoContainer<IOperator>;
public:
    using TBase::TBase;

    const TPortionStorageCursor& GetCursorVerified() const {
        AFL_VERIFY(Cursor);
        return *Cursor;
    }

    void SetCursor(const TPortionStorageCursor& cursor) {
        AFL_VERIFY(!Cursor);
        Cursor = cursor;
    }

    std::shared_ptr<arrow::Scalar> GetScalarVerified(const TPortionStorage& storage) {
        AFL_VERIFY(!!Cursor);
        return storage.GetScalarVerified(*Cursor);
    }
};

}