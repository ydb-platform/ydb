#pragma once
#include <ydb/core/tx/columnshard/counters/scan.h>
#include <ydb/core/tx/columnshard/engines/reader/abstract/read_metadata.h>
#include <ydb/core/tx/columnshard/engines/reader/common/conveyor_task.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/columns_set.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/fetching.h>
#include <ydb/core/tx/columnshard/engines/scheme/abstract_scheme.h>
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/abstract.h>

#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NOlap::NReader::NPlain {

using TColumnsSet = NCommon::TColumnsSet;
using TIndexesSet = NCommon::TIndexesSet;
using EStageFeaturesIndexes = NCommon::EStageFeaturesIndexes;
using TColumnsSetIds = NCommon::TColumnsSetIds;
using EMemType = NCommon::EMemType;
using TFetchingScriptCursor = NCommon::TFetchingScriptCursor;
using TStepAction = NCommon::TStepAction;

class IDataSource;
class TSpecialReadContext;
class IFetchingStep: public NCommon::IFetchingStep {
private:
    using TBase = NCommon::IFetchingStep;
    virtual TConclusion<bool> DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& step) const = 0;
    virtual ui64 GetProcessingDataSize(const std::shared_ptr<IDataSource>& /*source*/) const {
        return 0;
    }

    virtual ui64 GetProcessingDataSize(const std::shared_ptr<NCommon::IDataSource>& source) const override final {
        return GetProcessingDataSize(std::static_pointer_cast<IDataSource>(source));
    }

    virtual TConclusion<bool> DoExecuteInplace(
        const std::shared_ptr<NCommon::IDataSource>& sourceExt, const TFetchingScriptCursor& step) const override final {
        const auto source = std::static_pointer_cast<IDataSource>(sourceExt);
        return DoExecuteInplace(source, step);
    }

public:
    using TBase::TBase;
};

class TBuildFakeSpec: public IFetchingStep {
private:
    using TBase = IFetchingStep;

protected:
    virtual TConclusion<bool> DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& step) const override;

public:
    TBuildFakeSpec()
        : TBase("FAKE_SPEC") {
    }
};

class TDetectInMemStep: public IFetchingStep {
private:
    using TBase = IFetchingStep;
    const TColumnsSetIds Columns;

protected:
    virtual TConclusion<bool> DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& step) const override;
    virtual TString DoDebugString() const override {
        return TStringBuilder() << "columns=" << Columns.DebugString() << ";";
    }

public:
    virtual ui64 GetProcessingDataSize(const std::shared_ptr<IDataSource>& source) const override;
    TDetectInMemStep(const TColumnsSetIds& columns)
        : TBase("FETCHING_COLUMNS")
        , Columns(columns) {
        AFL_VERIFY(Columns.GetColumnsCount());
    }
};

class TPortionAccessorFetchingStep: public IFetchingStep {
private:
    using TBase = IFetchingStep;

protected:
    virtual TConclusion<bool> DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& step) const override;
    virtual TString DoDebugString() const override {
        return TStringBuilder();
    }

public:
    TPortionAccessorFetchingStep()
        : TBase("FETCHING_ACCESSOR") {
    }
};

class TFilterCutLimit: public IFetchingStep {
private:
    using TBase = IFetchingStep;
    const ui32 Limit;
    const bool Reverse;

public:
    virtual TConclusion<bool> DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& step) const override;
    TFilterCutLimit(const ui32 limit, const bool reverse)
        : TBase("LIMIT")
        , Limit(limit)
        , Reverse(reverse) {
        AFL_VERIFY(Limit);
    }
};

class TPredicateFilter: public IFetchingStep {
private:
    using TBase = IFetchingStep;

public:
    virtual TConclusion<bool> DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& step) const override;
    TPredicateFilter()
        : TBase("PREDICATE") {
    }
};

class TSnapshotFilter: public IFetchingStep {
private:
    using TBase = IFetchingStep;

public:
    virtual TConclusion<bool> DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& step) const override;
    TSnapshotFilter()
        : TBase("SNAPSHOT") {
    }
};

class TDetectInMem: public IFetchingStep {
private:
    using TBase = IFetchingStep;
    TColumnsSetIds Columns;

public:
    virtual TConclusion<bool> DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& step) const override;
    TDetectInMem(const TColumnsSetIds& columns)
        : TBase("DETECT_IN_MEM")
        , Columns(columns) {
    }
};

class TDeletionFilter: public IFetchingStep {
private:
    using TBase = IFetchingStep;

public:
    virtual TConclusion<bool> DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& step) const override;
    TDeletionFilter()
        : TBase("DELETION") {
    }
};

class TShardingFilter: public IFetchingStep {
private:
    using TBase = IFetchingStep;

public:
    virtual TConclusion<bool> DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& step) const override;
    TShardingFilter()
        : TBase("SHARDING") {
    }
};

}   // namespace NKikimr::NOlap::NReader::NPlain
