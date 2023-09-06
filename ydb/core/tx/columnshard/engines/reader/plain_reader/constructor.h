#pragma once
#include <ydb/core/tx/columnshard/engines/portions/column_record.h>
#include <ydb/core/tx/columnshard/blob.h>
#include "source.h"

namespace NKikimr::NOlap::NPlainReader {

class IFetchTaskConstructor {
private:
    bool Constructed = false;
    IDataReader& Reader;
    bool Started = false;
protected:
    THashSet<TBlobRange> WaitingData;
    THashMap<TBlobRange, TPortionInfo::TAssembleBlobInfo> Data;
    virtual void DoOnDataReady(IDataReader& reader) = 0;

    void OnDataReady(IDataReader& reader) {
        if (WaitingData.empty()) {
            Constructed = true;
            return DoOnDataReady(reader);
        }
    }
public:
    IFetchTaskConstructor(IDataReader& reader)
        : Reader(reader)
    {

    }

    void StartDataWaiting() {
        Started = true;
        OnDataReady(Reader);
    }

    void Abort() {
        Constructed = true;
    }

    virtual ~IFetchTaskConstructor() {
        Y_VERIFY(Constructed);
    }

    void AddWaitingRecord(const TColumnRecord& rec) {
        Y_VERIFY(!Started);
        Y_VERIFY(WaitingData.emplace(rec.BlobRange).second);
    }

    void AddData(const TBlobRange& range, TString&& data) {
        Y_VERIFY(Started);
        Y_VERIFY(WaitingData.erase(range));
        Y_VERIFY(Data.emplace(range, std::move(data)).second);
        OnDataReady(Reader);
    }

    void AddNullData(const TBlobRange& range, const ui32 rowsCount) {
        Y_VERIFY(!Started);
        Y_VERIFY(Data.emplace(range, rowsCount).second);
    }
};

class TAssembleColumnsTaskConstructor: public IFetchTaskConstructor {
private:
    using TBase = IFetchTaskConstructor;
protected:
    std::set<ui32> ColumnIds;
    const ui32 SourceIdx;
    std::shared_ptr<TPortionInfo> PortionInfo;
    TPortionInfo::TPreparedBatchData BuildBatchAssembler(IDataReader& reader);
public:
    TAssembleColumnsTaskConstructor(const std::set<ui32>& columnIds, const TPortionDataSource& portion, IDataReader& reader)
        : TBase(reader)
        , ColumnIds(columnIds)
        , SourceIdx(portion.GetSourceIdx())
        , PortionInfo(portion.GetPortionInfoPtr())
    {

    }
};

class TFFColumnsTaskConstructor: public TAssembleColumnsTaskConstructor {
private:
    using TBase = TAssembleColumnsTaskConstructor;
    std::shared_ptr<NArrow::TColumnFilter> AppliedFilter;
    virtual void DoOnDataReady(IDataReader& reader) override;
public:
    TFFColumnsTaskConstructor(const std::set<ui32>& columnIds, const TPortionDataSource& portion, IDataReader& reader)
        : TBase(columnIds, portion, reader)
        , AppliedFilter(portion.GetFilterStageData().GetAppliedFilter())
    {
    }
};

class TEFTaskConstructor: public TAssembleColumnsTaskConstructor {
private:
    bool UseEarlyFilter = false;
    using TBase = TAssembleColumnsTaskConstructor;
    virtual void DoOnDataReady(IDataReader& reader) override;
public:
    TEFTaskConstructor(const std::set<ui32>& columnIds, const TPortionDataSource& portion, IDataReader& reader, const bool useEarlyFilter)
        : TBase(columnIds, portion, reader)
        , UseEarlyFilter(useEarlyFilter)
    {
    }
};

}
