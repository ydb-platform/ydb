#pragma once

#include "yql_data_provider.h"
#include "yql_type_annotation.h"
#include <ydb/library/yql/ast/yql_gc_nodes.h>
#include <util/system/mutex.h>

#ifndef YQL_OPERATION_STATISTICS_CUSTOM_FIELDS
#define YQL_OPERATION_STATISTICS_CUSTOM_FIELDS
#endif

namespace NYql {
    struct TOperationProgress {
#define YQL_OPERATION_PROGRESS_STATE_MAP(xx) \
    xx(Started, 0)                           \
    xx(InProgress, 1)                        \
    xx(Finished, 2)                          \
    xx(Failed, 3)                            \
    xx(Aborted, 4)

        enum class EState {
            YQL_OPERATION_PROGRESS_STATE_MAP(ENUM_VALUE_GEN)
        };

#define YQL_OPERATION_BLOCK_STATUS_MAP(xx) \
    xx(None, 0)                            \
    xx(Partial, 1)                         \
    xx(Full, 2)

        enum class EOpBlockStatus {
            YQL_OPERATION_BLOCK_STATUS_MAP(ENUM_VALUE_GEN)
        };

        TString Category;
        ui32 Id;
        EState State;
        TMaybe<EOpBlockStatus> BlockStatus;

        using TStage = std::pair<TString, TInstant>;
        TStage Stage;

        TString RemoteId;
        THashMap<TString, TString> RemoteData;

        struct TCounters {
            ui64 Completed = 0ULL;
            ui64 Running = 0ULL;
            ui64 Total = 0ULL;
            ui64 Aborted = 0ULL;
            ui64 Failed = 0ULL;
            ui64 Lost = 0ULL;
            ui64 Pending = 0ULL;
            THashMap<TString, i64> Custom = {};
            bool operator==(const TCounters& rhs) const noexcept {
                return Completed == rhs.Completed &&
                       Running == rhs.Running &&
                       Total == rhs.Total &&
                       Aborted == rhs.Aborted &&
                       Failed == rhs.Failed &&
                       Lost == rhs.Lost &&
                       Pending == rhs.Pending &&
                       Custom == rhs.Custom;
            }

            bool operator!=(const TCounters& rhs) const noexcept {
                return !operator==(rhs);
            }
        };

        TMaybe<TCounters> Counters;

        TOperationProgress(const TString& category, ui32 id,
            EState state, const TString& stage = "")
            : Category(category)
            , Id(id)
            , State(state)
            , Stage(stage, TInstant::Now())
        {
        }
    };

    struct TOperationStatistics {
        struct TEntry {
            TString Name;

            TMaybe<i64> Sum;
            TMaybe<i64> Max;
            TMaybe<i64> Min;
            TMaybe<i64> Avg;
            TMaybe<i64> Count;
            TMaybe<TString> Value;

            TEntry(TString name, TMaybe<i64> sum, TMaybe<i64> max, TMaybe<i64> min, TMaybe<i64> avg, TMaybe<i64> count)
                : Name(std::move(name))
                , Sum(std::move(sum))
                , Max(std::move(max))
                , Min(std::move(min))
                , Avg(std::move(avg))
                , Count(std::move(count))
            {
            }

            TEntry(TString name, TString value)
                : Name(std::move(name))
                , Value(std::move(value))
            {
            }
        };

        TVector<TEntry> Entries;
    };

    using TStatWriter = std::function<void(ui32, const TVector<TOperationStatistics::TEntry>&)>;
    using TOperationProgressWriter = std::function<void(const TOperationProgress&)>;

    inline TStatWriter ThreadSafeStatWriter(TStatWriter base) {
        struct TState : public TThrRefBase {
            TStatWriter Base;
            TMutex Mutex;
        };

        auto state = MakeIntrusive<TState>();
        state->Base = base;
        return [state](ui32 id, const TVector<TOperationStatistics::TEntry>& stat) {
            with_lock(state->Mutex) {
                state->Base(id, stat);
            }
        };
    }

    inline void NullProgressWriter(const TOperationProgress& progress) {
        Y_UNUSED(progress);
    }

    inline TOperationProgressWriter ChainProgressWriters(TOperationProgressWriter left, TOperationProgressWriter right) {
        return [=](const TOperationProgress& progress) {
            left(progress);
            right(progress);
        };
    }

    inline TOperationProgressWriter ThreadSafeProgressWriter(TOperationProgressWriter base) {
        struct TState : public TThrRefBase {
            TOperationProgressWriter Base;
            TMutex Mutex;
        };

        auto state = MakeIntrusive<TState>();
        state->Base = base;
        return [state](const TOperationProgress& progress) {
            with_lock(state->Mutex) {
                state->Base(progress);
            }
        };
    }

    TAutoPtr<IGraphTransformer> CreateCheckExecutionTransformer(const TTypeAnnotationContext& types, bool checkWorld = true);
    TAutoPtr<IGraphTransformer> CreateExecutionTransformer(TTypeAnnotationContext& types, TOperationProgressWriter writer, bool withFinalize = true);

    IGraphTransformer::TStatus RequireChild(const TExprNode& node, ui32 index);
}
