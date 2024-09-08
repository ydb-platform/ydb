#pragma once

#include <ydb/core/kqp/common/simple/kqp_event_ids.h>
#include <ydb/library/yql/dq/actors/protos/dq_status_codes.pb.h>
#include <ydb/library/yql/public/issue/yql_issue.h>


namespace NKikimr {
namespace NKqp {

struct TPrepareSettings {
    ui64 TxId;
    THashSet<ui64> SendingShards;
    THashSet<ui64> ReceivingShards;
    std::optional<ui64> ArbiterShard;
};

struct TPreparedInfo {
    ui64 ShardId;
    ui64 MinStep;
    ui64 MaxStep;
    TVector<ui64> Coordinators;
};

struct TEvKqpBuffer {

struct TEvPrepare : public TEventLocal<TEvPrepare, TKqpBufferWriterEvents::EvPrepare> {
    TPrepareSettings Settings;
};

struct TEvPrepared : public TEventLocal<TEvPrepared, TKqpBufferWriterEvents::EvPrepared> {
    TPreparedInfo Result;
};

struct TEvCommit : public TEventLocal<TEvCommit, TKqpBufferWriterEvents::EvCommit> {
};

struct TEvCommitted : public TEventLocal<TEvCommitted, TKqpBufferWriterEvents::EvCommitted> {
    ui64 ShardId;
};

struct TEvRollback : public TEventLocal<TEvRollback, TKqpBufferWriterEvents::EvRollback> {
};

struct TEvFlush : public TEventLocal<TEvFlush, TKqpBufferWriterEvents::EvFlush> {
};

struct TEvError : public TEventLocal<TEvError, TKqpBufferWriterEvents::EvError> {
    TString Message;
    NYql::NDqProto::StatusIds::StatusCode StatusCode;
    NYql::TIssues SubIssues;

    TEvError(const TString& message, NYql::NDqProto::StatusIds::StatusCode statusCode, const NYql::TIssues& subIssues);
};

struct TEvTerminate : public TEventLocal<TEvTerminate, TKqpBufferWriterEvents::EvTerminate> {
};

};

}
}
