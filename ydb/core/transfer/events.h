#pragma once

#include <ydb/core/base/defs.h>
#include <ydb/core/base/events.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>
#include <yql/essentials/public/issue/yql_issue.h>

namespace NKikimr::NReplication::NTransfer {

enum EEv {
    EvBegin = EventSpaceBegin(TKikimrEvents::ES_TRANSFER),

    EvWriteCompleeted,
    EvRetryTable,

    EvEnd,
};

namespace NTransferPrivate {

struct TEvWriteCompleeted: public TEventLocal<TEvWriteCompleeted, EEv::EvWriteCompleeted> {
    TEvWriteCompleeted(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues)
        : Status(status)
        , Issues(std::move(issues))
    {
    }

    const Ydb::StatusIds::StatusCode Status; 
    const NYql::TIssues Issues;
};

struct TEvRetryTable: public TEventLocal<TEvRetryTable, EEv::EvRetryTable> {
    TEvRetryTable(const TString& tablePath)
        : TablePath(tablePath)
    {
    }

    const TString TablePath;
};

}
}
