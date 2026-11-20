#pragma once

#include <ydb/core/base/events.h>
#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>
#include <yql/essentials/public/issue/yql_issue.h>

#include <variant>

namespace NKikimr {
namespace NSequenceProxy {

    inline TActorId MakeSequenceProxyServiceID(ui32 nodeId = 0) {
        return TActorId(nodeId, TStringBuf("seqproxy_svc"));
    }

    struct TEvSequenceProxy {
        enum EEv {
            EvNextVal = EventSpaceBegin(TKikimrEvents::ES_SEQUENCEPROXY),
            EvNextValResult,
            EvSetVal,
            EvSetValResult,
            EvGetSequence,
            EvGetSequenceResult,
            EvAdvanceSequence,
            EvAdvanceSequenceResult,
            EvEnd,
        };

        static_assert(TKikimrEvents::ES_SEQUENCEPROXY == 4217,
            "Expected TKikimrEvents::ES_SEQUENCEPROXY == 4217");
        static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_SEQUENCEPROXY),
            "Expected EvEnd < EventSpaceEnd(TKikimrEvents::ES_SEQUENCEPROXY)");

        struct TEvNextVal : public TEventLocal<TEvNextVal, EvNextVal> {
            TString Database;
            std::variant<TString, TPathId> Path;
            TIntrusivePtr<NACLib::TUserToken> UserToken;

            explicit TEvNextVal(const TString& path)
                : Path(path)
            { }

            explicit TEvNextVal(const TPathId& pathId)
                : Path(pathId)
            { }

            TEvNextVal(const TString& database, const TString& path)
                : Database(database)
                , Path(path)
            { }

            TEvNextVal(const TString& database, const TPathId& pathId)
                : Database(database)
                , Path(pathId)
            { }
        };

        struct TEvNextValResult : public TEventLocal<TEvNextValResult, EvNextValResult> {
            Ydb::StatusIds::StatusCode Status;
            NYql::TIssues Issues;
            TPathId PathId;
            i64 Value;

            TEvNextValResult(Ydb::StatusIds::StatusCode status, const NYql::TIssues& issues)
                : Status(status)
                , Issues(issues)
                , Value(0)
            { }

            TEvNextValResult(const TPathId& pathId, i64 value)
                : Status(Ydb::StatusIds::SUCCESS)
                , PathId(pathId)
                , Value(value)
            { }
        };

        struct TEvGetSequence : public TEventLocal<TEvGetSequence, EvGetSequence> {
            TString Database;
            TPathId PathId;
            TIntrusivePtr<NACLib::TUserToken> UserToken;

            explicit TEvGetSequence(const TPathId& pathId)
                : PathId(pathId)
            { }

            TEvGetSequence(const TString& database, const TPathId& pathId)
                : Database(database)
                , PathId(pathId)
            { }
        };

        struct TEvGetSequenceResult : public TEventLocal<TEvGetSequenceResult, EvGetSequenceResult> {
            Ydb::StatusIds::StatusCode Status;
            NYql::TIssues Issues;
            TPathId PathId;

            i64 MinValue;
            i64 MaxValue;
            i64 StartValue;
            i64 NextValue;
            bool NextUsed;
            ui64 Cache;
            i64 Increment;
            bool Cycle;

            TEvGetSequenceResult(const TPathId& pathId)
                : Status(Ydb::StatusIds::SUCCESS)
                , PathId(pathId)
                , MinValue(0)
                , MaxValue(0)
                , StartValue(0)
                , NextValue(0)
                , NextUsed(false)
                , Cache(0)
                , Increment(0)
                , Cycle(false)
            { }

            TEvGetSequenceResult(Ydb::StatusIds::StatusCode status, const NYql::TIssues& issues)
                : Status(status)
                , Issues(issues)
                , MinValue(0)
                , MaxValue(0)
                , StartValue(0)
                , NextValue(0)
                , NextUsed(false)
                , Cache(0)
                , Increment(0)
                , Cycle(false)
            { }

            explicit TEvGetSequenceResult(const TEvGetSequenceResult& ev)
                : Status(ev.Status)
                , Issues(ev.Issues)
                , PathId(ev.PathId)
                , MinValue(ev.MinValue)
                , MaxValue(ev.MaxValue)
                , StartValue(ev.StartValue)
                , NextValue(ev.NextValue)
                , NextUsed(ev.NextUsed)
                , Cache(ev.Cache)
                , Increment(ev.Increment)
                , Cycle(ev.Cycle)
            { }
        };

        // Moves a sequence past the specified range of values: the sequence is advanced so that
        // everything within [ObservedMin, ObservedMax] counts as already handed out.
        struct TEvAdvanceSequence : public TEventLocal<TEvAdvanceSequence, EvAdvanceSequence> {
            TString Database;
            std::variant<TString, TPathId> Path;
            i64 ObservedMin = 0;
            i64 ObservedMax = 0;

            TEvAdvanceSequence(const TString& database, const TString& path, i64 observedMin, i64 observedMax)
                : Database(database)
                , Path(path)
                , ObservedMin(observedMin)
                , ObservedMax(observedMax)
            { }

            TEvAdvanceSequence(const TString& database, const TPathId& pathId, i64 observedMin, i64 observedMax)
                : Database(database)
                , Path(pathId)
                , ObservedMin(observedMin)
                , ObservedMax(observedMax)
            { }
        };

        struct TEvAdvanceSequenceResult : public TEventLocal<TEvAdvanceSequenceResult, EvAdvanceSequenceResult> {
            Ydb::StatusIds::StatusCode Status;
            NYql::TIssues Issues;
            TPathId PathId;
            // State after the advance, useful for logging and tests.
            i64 NextValue = 0;
            bool NextUsed = false;

            TEvAdvanceSequenceResult(const TPathId& pathId, i64 nextValue, bool nextUsed)
                : Status(Ydb::StatusIds::SUCCESS)
                , PathId(pathId)
                , NextValue(nextValue)
                , NextUsed(nextUsed)
            { }

            TEvAdvanceSequenceResult(Ydb::StatusIds::StatusCode status, const NYql::TIssues& issues)
                : Status(status)
                , Issues(issues)
            { }
        };
    };

} // namespace NSequenceProxy
} // namespace NKikimr
