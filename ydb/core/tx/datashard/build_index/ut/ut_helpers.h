#pragma once
#include "defs.h"
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/tx/datashard/datashard_ut_common_kqp.h>
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include <ydb/library/actors/core/event.h>

#include <yql/essentials/public/issue/yql_issue_message.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/string.h>

namespace NKikimr {

using namespace NKikimr::NDataShard::NKqpHelpers;
using namespace NSchemeShard;
using namespace Tests;

static const TString kMainTable = "/Root/table-1";
static const TString kIndexTable = "/Root/table-2";

template <class TResponse>
typename TResponse::TPtr DoBadRequest(Tests::TServer::TPtr server, TActorId sender,
    NActors::IEventBase* ev, ui64 tabletId,
    TString expectedError, bool expectedErrorSubstring = false)
{
    auto& runtime = *server->GetRuntime();
    runtime.SendToPipe(tabletId, sender, ev, 0, GetPipeConfigWithRetries());

    auto reply = runtime.GrabEdgeEventRethrow<TResponse>(sender);

    auto status = reply->Get()->Record.GetStatus();
    UNIT_ASSERT_VALUES_EQUAL_C(status, NKikimrIndexBuilder::EBuildStatus::BAD_REQUEST, "Expected error: " << expectedError);

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
