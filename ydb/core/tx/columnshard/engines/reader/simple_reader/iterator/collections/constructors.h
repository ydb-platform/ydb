#pragma once
#include "abstract.h"

#include <ydb/library/accessor/positive_integer.h>

namespace NKikimr::NOlap::NReader::NSimple {

class TNotSortedPortionsSources: public ISourcesConstructor {
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

class TSortedPortionsSources: public ISourcesConstructor {
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
