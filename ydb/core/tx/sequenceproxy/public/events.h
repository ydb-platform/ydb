#pragma once

#include <ydb/core/base/events.h>
#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>
#include <ydb/library/yql/public/issue/yql_issue.h>

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

        struct TEvNextValResult : public TEventLocal<TEvNextValResult, EvNextVal> {
            Ydb::StatusIds::StatusCode Status;
            NYql::TIssues Issues;
            TPathId PathId;
            i64 Value;

            TEvNextValResult(Ydb::StatusIds::StatusCode status, const NYql::TIssues& issues)
                : Status(status)
                , Issues(issues)
            { }

            TEvNextValResult(const TPathId& pathId, i64 value)
                : Status(Ydb::StatusIds::SUCCESS)
                , PathId(pathId)
                , Value(value)
            { }
        };
    };

} // namespace NSequenceProxy
} // namespace NKikimr
