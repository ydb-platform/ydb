#pragma once

#include <ydb/core/kqp/common/simple/kqp_event_ids.h>
#include <ydb/library/yql/dq/actors/protos/dq_stats.pb.h>
#include <ydb/library/yql/dq/actors/protos/dq_status_codes.pb.h>
#include <yql/essentials/public/issue/yql_issue.h>


namespace NKikimr {
namespace NKqp {

struct TEvKqpBuffer {

struct TEvPrepare : public TEventLocal<TEvPrepare, TKqpBufferWriterEvents::EvPrepare> {
    TActorId ExecuterActorId;
};

struct TEvCommit : public TEventLocal<TEvCommit, TKqpBufferWriterEvents::EvCommit> {
    TActorId ExecuterActorId;
    ui64 TxId;
};

struct TEvRollback : public TEventLocal<TEvRollback, TKqpBufferWriterEvents::EvRollback> {
    TActorId ExecuterActorId;
};

struct TEvFlush : public TEventLocal<TEvFlush, TKqpBufferWriterEvents::EvFlush> {
    TActorId ExecuterActorId;
};

struct TEvResult : public TEventLocal<TEvResult, TKqpBufferWriterEvents::EvResult> {
    TEvResult() = default;
    TEvResult(NYql::NDqProto::TDqTaskStats&& stats) : Stats(std::move(stats)) {}

    std::optional<NYql::NDqProto::TDqTaskStats> Stats;
};

struct TEvError : public TEventLocal<TEvError, TKqpBufferWriterEvents::EvError> {
    NYql::NDqProto::StatusIds::StatusCode StatusCode;
    NYql::TIssues Issues;

    TEvError(NYql::NDqProto::StatusIds::StatusCode statusCode, NYql::TIssues&& issues);
};

struct TEvTerminate : public TEventLocal<TEvTerminate, TKqpBufferWriterEvents::EvTerminate> {
};

};

}
}
