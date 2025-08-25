#pragma once
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/tx/datashard/datashard_ut_common_kqp.h>
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include <ydb/library/actors/core/event.h>

#include <yql/essentials/public/issue/yql_issue_message.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/string.h>

namespace NKikimr {

template <class TResponse>
typename TResponse::TPtr DoBadRequest(Tests::TServer::TPtr server, TActorId sender,
    std::unique_ptr<NActors::IEventBase> ev, ui64 tabletId,
    const TString& expectedError, bool expectedErrorSubstring = false,
    NKikimrIndexBuilder::EBuildStatus expectedStatus = NKikimrIndexBuilder::EBuildStatus::BAD_REQUEST)
{
    auto& runtime = *server->GetRuntime();
    runtime.SendToPipe(tabletId, sender, ev.release(), 0, GetPipeConfigWithRetries());

    auto reply = runtime.GrabEdgeEventRethrow<TResponse>(sender);

    auto status = reply->Get()->Record.GetStatus();
    UNIT_ASSERT_VALUES_EQUAL_C(status, expectedStatus, "Expected error: " << expectedError);

    NYql::TIssues issues;
    NYql::IssuesFromMessage(reply->Get()->Record.GetIssues(), issues);
    if (expectedErrorSubstring) {
        UNIT_ASSERT_STRING_CONTAINS(issues.ToOneLineString(), expectedError);
    } else {
        UNIT_ASSERT_VALUES_EQUAL(issues.ToOneLineString(), expectedError);
    }

    return reply;
}

} // namespace NKikimr
