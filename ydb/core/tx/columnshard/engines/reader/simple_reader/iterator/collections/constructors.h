#pragma once
#include "abstract.h"

#include <ydb/core/tx/columnshard/engines/reader/common_reader/constructor/read_metadata.h>

#include <ydb/library/accessor/positive_integer.h>

namespace NKikimr::NOlap {
class TPortionInfo;
}

namespace NKikimr::NOlap::NReader::NSimple {

class TSourceConstructor: public ICursorEntity, public TMoveOnly {
private:
    TCompareKeyForScanSequence Start;
    YDB_READONLY(ui32, SourceId, 0);
    YDB_READONLY_DEF(std::shared_ptr<TPortionInfo>, Portion);
    ui32 RecordsCount = 0;
    bool IsStartedByCursorFlag = false;

    virtual ui64 DoGetEntityId() const override {
        return SourceId;
    }
    virtual ui64 DoGetEntityRecordsCount() const override {
        return RecordsCount;
    }

public:
    void SetIsStartedByCursor() {
        IsStartedByCursorFlag = true;
    }
    bool GetIsStartedByCursor() const {
        return IsStartedByCursorFlag;
    }

    const TCompareKeyForScanSequence& GetStart() const {
        return Start;
    }

    TSourceConstructor(const std::shared_ptr<TPortionInfo>&& portion, const NReader::ERequestSorting sorting)
        : Start(TReplaceKeyAdapter((sorting == NReader::ERequestSorting::DESC) ? portion->IndexKeyEnd() : portion->IndexKeyStart(),
                    sorting == NReader::ERequestSorting::DESC),
              portion->GetPortionId())
        , SourceId(portion->GetPortionId())
        , Portion(std::move(portion))
        , RecordsCount(portion->GetRecordsCount()) {
    }

    bool operator<(const TSourceConstructor& item) const {
        return item.Start < Start;
    }

    std::shared_ptr<TPortionDataSource> Construct(const ui32 sourceIdx, const std::shared_ptr<TSpecialReadContext>& context) const;
};

class TNotSortedPortionsSources: public NCommon::ISourcesConstructor {
private:
    std::deque<TSourceConstructor> Sources;
    ui32 SourceIdx = 0;

    virtual void DoFillReadStats(TReadStats& stats) const override {
        ui64 compactedPortionsBytes = 0;
        ui64 insertedPortionsBytes = 0;
        ui64 committedPortionsBytes = 0;
        for (auto&& i : Sources) {
            if (i.GetPortion()->GetPortionType() == EPortionType::Compacted) {
                compactedPortionsBytes += i.GetPortion()->GetTotalBlobBytes();
            } else if (i.GetPortion()->GetProduced() == NPortion::EProduced::INSERTED) {
                insertedPortionsBytes += i.GetPortion()->GetTotalBlobBytes();
            } else {
                committedPortionsBytes += i.GetPortion()->GetTotalBlobBytes();
            }
        }
        stats.IndexPortions = Sources.size();
        stats.InsertedPortionsBytes = insertedPortionsBytes;
        stats.CompactedPortionsBytes = compactedPortionsBytes;
        stats.CommittedPortionsBytes = committedPortionsBytes;
    }

    virtual TString DoDebugString() const override {
        return "{" + ::ToString(Sources.size()) + "}";
    }

    virtual void DoInitCursor(const std::shared_ptr<IScanCursor>& cursor) override {
        while (Sources.size()) {
            bool usage = false;
            if (!cursor->CheckEntityIsBorder(Sources.front(), usage)) {
                Sources.pop_front();
                continue;
            }
            if (usage) {
                Sources.front().SetIsStartedByCursor();
            } else {
                Sources.pop_front();
            }
            break;
        }
    }

    virtual void DoClear() override {
        Sources.clear();
    }
    virtual void DoAbort() override {
        Sources.clear();
    }
    virtual bool DoIsFinished() const override {
        return Sources.empty();
    }
    virtual std::shared_ptr<NCommon::IDataSource> DoExtractNext(
        const std::shared_ptr<NCommon::TSpecialReadContext>& context, const ui32 /*inFlightCurrentLimit*/) override {
        auto result = Sources.front().Construct(SourceIdx++, static_pointer_cast<TSpecialReadContext>(context));
        Sources.pop_front();
        return result;
    }

public:
    TNotSortedPortionsSources() = default;

    virtual std::vector<TInsertWriteId> GetUncommittedWriteIds() const override;

    TNotSortedPortionsSources(std::deque<TSourceConstructor>&& sources)
        : Sources(std::move(sources)) {
    }
};

class TSortedPortionsSources: public NCommon::ISourcesConstructor {
private:
    std::deque<TSourceConstructor> HeapSources;
    ui32 SourceIdx = 0;

    virtual void DoFillReadStats(TReadStats& stats) const override {
        ui64 compactedPortionsBytes = 0;
        ui64 insertedPortionsBytes = 0;
        ui64 committedPortionsBytes = 0;
        for (auto&& i : HeapSources) {
            if (i.GetPortion()->GetPortionType() == EPortionType::Compacted) {
                compactedPortionsBytes += i.GetPortion()->GetTotalBlobBytes();
            } else if (i.GetPortion()->GetProduced() == NPortion::EProduced::INSERTED) {
                insertedPortionsBytes += i.GetPortion()->GetTotalBlobBytes();
            } else {
                committedPortionsBytes += i.GetPortion()->GetTotalBlobBytes();
            }
        }
        stats.IndexPortions = HeapSources.size();
        stats.InsertedPortionsBytes = insertedPortionsBytes;
        stats.CompactedPortionsBytes = compactedPortionsBytes;
        stats.CommittedPortionsBytes = committedPortionsBytes;
    }

    virtual TString DoDebugString() const override {
        return "{" + ::ToString(HeapSources.size()) + "}";
    }

    virtual void DoInitCursor(const std::shared_ptr<IScanCursor>& cursor) override;

    virtual std::vector<TInsertWriteId> GetUncommittedWriteIds() const override;

    virtual void DoClear() override {
        HeapSources.clear();
    }
    virtual void DoAbort() override {
        HeapSources.clear();
    }
    virtual bool DoIsFinished() const override {
        return HeapSources.empty();
    }
    virtual std::shared_ptr<NCommon::IDataSource> DoExtractNext(
        const std::shared_ptr<NCommon::TSpecialReadContext>& context, const ui32 /*inFlightCurrentLimit*/) override {
        AFL_VERIFY(HeapSources.size());
        std::pop_heap(HeapSources.begin(), HeapSources.end());
        auto result = HeapSources.back().Construct(SourceIdx++, static_pointer_cast<TSpecialReadContext>(context));
        HeapSources.pop_back();
        return result;
    }

public:
    TSortedPortionsSources(std::deque<TSourceConstructor>&& sources)
        : HeapSources(std::move(sources)) {
        std::make_heap(HeapSources.begin(), HeapSources.end());
    }
};

}   // namespace NKikimr::NOlap::NReader::NSimple
