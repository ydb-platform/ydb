#pragma once
#include "schema.h"
#include "source.h"

#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/constructor/read_metadata.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/context.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/source.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/sys_view/abstract/constructor.h>

namespace NKikimr::NOlap::NReader::NSimple::NSysView::NChunks {
class TPortionDataConstructor {
private:
    NColumnShard::TUnifiedPathId PathId;
    ui64 TabletId;
    YDB_READONLY_DEF(TPortionInfo::TConstPtr, Portion);
    ISnapshotSchema::TPtr Schema;
    NArrow::TSimpleRow Start;
    NArrow::TSimpleRow Finish;
    ui32 SourceId = 0;
    ui32 SourceIdx = 0;

public:
    void SetIndex(const ui32 index) {
        AFL_VERIFY(!SourceId);
        SourceIdx = index;
        SourceId = index + 1;
    }

    TPortionDataConstructor(const NColumnShard::TUnifiedPathId& pathId, const ui64 tabletId, const TPortionInfo::TConstPtr& portion,
        const ISnapshotSchema::TPtr& schema)
        : PathId(pathId)
        , TabletId(tabletId)
        , Portion(portion)
        , Schema(schema)
        , Start(TSchemaAdapter::GetPKSimpleRow(PathId, TabletId, Portion->GetPortionId(), 0, 0))
        , Finish(TSchemaAdapter::GetPKSimpleRow(PathId, TabletId, Portion->GetPortionId(), Max<ui32>(), Max<ui32>())) {
    }

    const NArrow::TSimpleRow& GetStart() const {
        return Start;
    }
    const NArrow::TSimpleRow& GetFinish() const {
        return Finish;
    }

    struct TComparator {
    private:
        const bool IsReverse;

    public:
        TComparator(const bool isReverse)
            : IsReverse(isReverse) {
        }

        bool operator()(const TPortionDataConstructor& l, const TPortionDataConstructor& r) const {
            if (IsReverse) {
                return r.Finish < l.Finish;
            } else {
                return l.Start < r.Start;
            }
        }
    };

    std::shared_ptr<NReader::NSimple::IDataSource> Construct(const std::shared_ptr<NReader::NSimple::TSpecialReadContext>& context);
};

class TConstructor: public NAbstract::ISourcesConstructor {
private:
    std::deque<TPortionDataConstructor> Constructors;

    virtual void DoClear() override {
        Constructors.clear();
    }
    virtual void DoAbort() override {
        Constructors.clear();
    }
    virtual bool DoIsFinished() const override {
        return Constructors.empty();
    }
    virtual std::shared_ptr<NReader::NCommon::IDataSource> DoExtractNext(
        const std::shared_ptr<NReader::NCommon::TSpecialReadContext>& context) override {
        AFL_VERIFY(Constructors.size());
        std::shared_ptr<NReader::NCommon::IDataSource> result =
            Constructors.front().Construct(std::static_pointer_cast<NReader::NSimple::TSpecialReadContext>(context));
        Constructors.pop_front();
        return result;
    }
    virtual void DoInitCursor(const std::shared_ptr<IScanCursor>& /*cursor*/) override {
        AFL_VERIFY(false);
    }
    virtual TString DoDebugString() const override {
        return Default<TString>();
    }

public:
    TConstructor(const NOlap::IPathIdTranslator& pathIdTranslator, const IColumnEngine& engine, const ui64 tabletId,
        const std::optional<NOlap::TInternalPathId> internalPathId, const TSnapshot reqSnapshot,
        const std::shared_ptr<NOlap::TPKRangesFilter>& pkFilter, const bool isReverseSort) {
        const TColumnEngineForLogs* engineImpl = dynamic_cast<const TColumnEngineForLogs*>(&engine);
        const TVersionedIndex& originalSchemaInfo = engineImpl->GetVersionedIndex();
        for (auto&& i : engineImpl->GetTables()) {
            if (internalPathId && *internalPathId != i.first) {
                continue;
            }
            for (auto&& [_, p] : i.second->GetPortions()) {
                if (reqSnapshot < p->RecordSnapshotMin()) {
                    continue;
                }
                Constructors.emplace_back(
                    pathIdTranslator.GetUnifiedByInternalVerified(p->GetPathId()), tabletId, p, p->GetSchema(originalSchemaInfo));
                if (!pkFilter->IsUsed(Constructors.back().GetStart(), Constructors.back().GetFinish())) {
                    Constructors.pop_back();
                }
            }
        }
        std::sort(Constructors.begin(), Constructors.end(), TPortionDataConstructor::TComparator(isReverseSort));
        for (ui32 idx = 0; idx < Constructors.size(); ++idx) {
            Constructors[idx].SetIndex(idx);
        }
    }
};
}   // namespace NKikimr::NOlap::NReader::NSimple::NSysView::NChunks
