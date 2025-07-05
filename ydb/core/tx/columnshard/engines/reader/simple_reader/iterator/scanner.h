#pragma once
#include "source.h"

#include "collections/abstract.h"
#include "sync_points/abstract.h"

#include <ydb/core/formats/arrow/reader/position.h>
#include <ydb/core/tx/columnshard/common/limits.h>
#include <ydb/core/tx/columnshard/engines/reader/abstract/read_context.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>

namespace NKikimr::NOlap::NReader::NSimple {

class TPlainReadData;

class TScanHead {
private:
    std::shared_ptr<TSpecialReadContext> Context;
    std::shared_ptr<ISourcesCollection> SourcesCollection;
    std::vector<std::shared_ptr<ISyncPoint>> SyncPoints;

public:
    const std::shared_ptr<ISyncPoint>& GetResultSyncPoint() const {
        return SyncPoints.back();
    }

    const std::shared_ptr<ISyncPoint>& GetSyncPoint(const ui32 index) const {
        AFL_VERIFY(index < SyncPoints.size());
        return SyncPoints[index];
    }

    ISourcesCollection& MutableSourcesCollection() const {
        return *SourcesCollection;
    }

    const ISourcesCollection& GetSourcesCollection() const {
        return *SourcesCollection;
    }

    ~TScanHead();

    bool IsReverse() const;
    void Abort();

    bool IsFinished() const {
        for (auto&& i : SyncPoints) {
            if (!i->IsFinished()) {
                return false;
            }
        }
        return SourcesCollection->IsFinished();
    }

    const TReadContext& GetContext() const;

    TString DebugString() const {
        TStringBuilder sb;
        sb << "S:{" << SourcesCollection->DebugString() << "};";
        sb << "SP:[";
        for (auto&& i : SyncPoints) {
            sb << "{" << i->DebugString() << "};";
        }
        sb << "]";
        return sb;
    }

    TConclusionStatus Start();

    TScanHead(std::unique_ptr<NCommon::ISourcesConstructor>&& sourcesConstructor, const std::shared_ptr<TSpecialReadContext>& context);

    [[nodiscard]] TConclusion<bool> BuildNextInterval();
};

}   // namespace NKikimr::NOlap::NReader::NSimple
