#pragma once

#include "datashard_ut_common.h"

namespace NKikimr {
namespace NDataShard {
namespace NKqpHelpers {

    inline THolder<NKqp::TEvKqp::TEvQueryRequest> MakeSimpleRequest(
            const TString& sql,
            const TString& database = {})
    {
        auto request = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();
        request->Record.MutableRequest()->MutableTxControl()->mutable_begin_tx()->mutable_serializable_read_write();
        request->Record.MutableRequest()->MutableTxControl()->set_commit_tx(true);
        request->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
        request->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_SQL_DML);
        request->Record.MutableRequest()->SetQuery(sql);
        if (!database.empty()) {
            request->Record.MutableRequest()->SetDatabase(database);
        }
        return request;
    }

    inline THolder<NKqp::TEvKqp::TEvQueryRequest> MakeSimpleStaleRoRequest(
            const TString& sql,
            const TString& database = {})
    {
        auto request = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();
        request->Record.MutableRequest()->MutableTxControl()->mutable_begin_tx()->mutable_stale_read_only();
        request->Record.MutableRequest()->MutableTxControl()->set_commit_tx(true);
        request->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
        request->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_SQL_DML);
        request->Record.MutableRequest()->SetQuery(sql);
        if (!database.empty()) {
            request->Record.MutableRequest()->SetDatabase(database);
        }
        return request;
    }

    inline THolder<NKqp::TEvKqp::TEvQueryRequest> MakeBeginRequest(
            const TString& sessionId,
            const TString& sql,
            const TString& database = {})
    {
        auto request = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();
        request->Record.MutableRequest()->SetSessionId(sessionId);
        request->Record.MutableRequest()->MutableTxControl()->mutable_begin_tx()->mutable_serializable_read_write();
        request->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
        request->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_SQL_DML);
        request->Record.MutableRequest()->SetQuery(sql);
        if (!database.empty()) {
            request->Record.MutableRequest()->SetDatabase(database);
        }
        return request;
    }

    inline THolder<NKqp::TEvKqp::TEvQueryRequest> MakeContinueRequest(
            const TString& sessionId,
            const TString& txId,
            const TString& sql,
            const TString& database = {})
    {
        auto request = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();
        request->Record.MutableRequest()->SetSessionId(sessionId);
        request->Record.MutableRequest()->MutableTxControl()->set_tx_id(txId);
        request->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
        request->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_SQL_DML);
        request->Record.MutableRequest()->SetQuery(sql);
        if (!database.empty()) {
            request->Record.MutableRequest()->SetDatabase(database);
        }
        return request;
    }

    inline THolder<NKqp::TEvKqp::TEvQueryRequest> MakeCommitRequest(
            const TString& sessionId,
            const TString& txId,
            const TString& sql,
            const TString& database = {})
    {
        auto request = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();
        request->Record.MutableRequest()->SetSessionId(sessionId);
        request->Record.MutableRequest()->MutableTxControl()->set_tx_id(txId);
        request->Record.MutableRequest()->MutableTxControl()->set_commit_tx(true);
        request->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
        request->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_SQL_DML);
        request->Record.MutableRequest()->SetQuery(sql);
        if (!database.empty()) {
            request->Record.MutableRequest()->SetDatabase(database);
        }
        return request;
    }

    inline void SendRequest(
            TTestActorRuntime& runtime,
            TActorId sender,
            THolder<NKqp::TEvKqp::TEvQueryRequest> request)
    {
        runtime.Send(
            new IEventHandle(NKqp::MakeKqpProxyID(runtime.GetNodeId()), sender, request.Release()),
            0, /* via actor system */ true);
    }

    inline NKqp::TEvKqp::TEvQueryResponse::TPtr ExecRequest(
            TTestActorRuntime& runtime,
            TActorId sender,
            THolder<NKqp::TEvKqp::TEvQueryRequest> request)
    {
        SendRequest(runtime, sender, std::move(request));
        return runtime.GrabEdgeEventRethrow<NKqp::TEvKqp::TEvQueryResponse>(sender);
    }

    inline TString CreateSession(TTestActorRuntime& runtime, TActorId sender, const TString& database = {}) {
        auto request = MakeHolder<NKqp::TEvKqp::TEvCreateSessionRequest>();
        if (!database.empty()) {
            request->Record.MutableRequest()->SetDatabase(database);
        }
        runtime.Send(
            new IEventHandle(NKqp::MakeKqpProxyID(runtime.GetNodeId()), sender, request.Release()),
            0, /* via actor system */ true);
        auto ev = runtime.GrabEdgeEventRethrow<NKqp::TEvKqp::TEvCreateSessionResponse>(sender);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetYdbStatus(), Ydb::StatusIds::SUCCESS);
        return ev->Get()->Record.GetResponse().GetSessionId();
    }

    inline THolder<NKqp::TEvKqp::TEvQueryRequest> MakeStreamRequest(
        const TActorId sender,
        const TString& sql,
        const bool collectStats = false)
    {
        auto request = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();
        request->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
        request->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_SQL_SCAN);
        request->Record.MutableRequest()->SetKeepSession(false);
        request->Record.MutableRequest()->SetQuery(sql);
        request->Record.MutableRequest()->SetProfile(collectStats);
        ActorIdToProto(sender, request->Record.MutableRequestActorId());
        return request;
    }

} // namespace NKqpHelpers
} // namespace NDataShard
} // namespace NKikimr
