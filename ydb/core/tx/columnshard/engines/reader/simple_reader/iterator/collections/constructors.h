#pragma once
#include "abstract.h"

#include <ydb/core/tx/columnshard/engines/reader/common_reader/constructor/read_metadata.h>

#include <ydb/library/accessor/positive_integer.h>

namespace NKikimr::NOlap {
class TPortionInfo;
}

namespace NKikimr::NOlap::NReader::NCommon {

class TSourceConstructor: public ICursorEntity {
private:
    TCompareKeyForScanSequence Start;
    YDB_READONLY(ui32, SourceId, 0);
    std::shared_ptr<TPortionInfo> Portion;
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

    TSourceConstructor(const std::shared_ptr<TPortionInfo>&& portion, const std::shared_ptr<TReadContext>& context)
        : Start(TReplaceKeyAdapter(context->GetReadMetadata()->IsDescSorted() ? portion->IndexKeyEnd() : portion->IndexKeyStart(),
                    context->GetReadMetadata()->IsDescSorted()),
              portion->GetPortionId())
        , SourceId(portion->GetPortionId())
        , Portion(std::move(portion))
        , RecordsCount(portion->GetRecordsCount()) {
    }

    bool operator<(const TSourceConstructor& item) const {
        return item.Start < Start;
    }

    std::shared_ptr<TPortionDataSource> Construct(const ui32 sourceIdx, const std::shared_ptr<TSpecialReadContext>& context) const {
        auto result = std::make_shared<TPortionDataSource>(sourceIdx, Portion, context);
        if (IsStartedByCursorFlag) {
            result->SetIsStartedByCursor();
        }
        FOR_DEBUG_LOG(NKikimrServices::COLUMNSHARD_SCAN_EVLOG, result->AddEvent("s"));
        return result;
    }
};

class TNotSortedPortionsSources: public NCommon::ISourcesConstructor {
private:
    std::deque<TSourceConstructor> Sources;
    ui32 SourceIdx = 0;

    virtual TString DoDebugString() const override {
        return "{" + ::ToString(Sources.size()) + "}";
    }

    virtual void DoInitCursor(const std::shared_ptr<IScanCursor>& cursor) override {
        while (sources.size()) {
            bool usage = false;
            if (!context->GetCommonContext()->GetScanCursor()->CheckEntityIsBorder(sources.front(), usage)) {
                sources.pop_front();
                continue;
            }
            if (usage) {
                sources.front().SetIsStartedByCursor();
            } else {
                sources.pop_front();
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
    virtual void DoIsFinished() override {
        return Sources.empty();
    }
    virtual std::shared_ptr<IDataSource> DoExtractNext(const std::shared_ptr<TSpecialReadContext>& context) override {
        return Sources.front().Construct(SourceIdx++, context);
    }

public:
    TNotSortedPortionsSources(std::deque<TSourceConstructor>&& sources)
        : Sources(sources) {
    }
};

class TSortedPortionsSources: public NCommon::ISourcesConstructor {
private:
    std::deque<TSourceConstructor> HeapSources;
    ui32 SourceIdx = 0;

    virtual TString DoDebugString() const override {
        return "{" + ::ToString(HeapSources.size()) + "}";
    }

    virtual void DoInitCursor(const std::shared_ptr<IScanCursor>& cursor) override;

    virtual void DoClear() override {
        HeapSources.clear();
    }
    virtual void DoAbort() override {
        HeapSources.clear();
    }
    virtual void DoIsFinished() override {
        return HeapSources.empty();
    }
    virtual std::shared_ptr<IDataSource> DoExtractNext(const std::shared_ptr<TSpecialReadContext>& context) override {
        AFL_VERIFY(HeapSources.size());
        std::pop_heap(HeapSources.begin(), HeapSources.end());
        auto result = NextSource ? NextSource : HeapSources.back().Construct(SourceIdx++, Context);
        HeapSources.pop_back();
    }

public:
    TSortedPortionsSources(std::deque<TSourceConstructor>&& sources)
        : Sources(sources) {
        HeapSources = std::move(sources);
        std::make_heap(HeapSources.begin(), HeapSources.end());
    }
};

}   // namespace NKikimr::NOlap::NReader::NSimple
