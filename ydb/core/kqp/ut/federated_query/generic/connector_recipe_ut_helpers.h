#pragma once
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/ut/federated_query/common/common.h>
#include <ydb/core/kqp/federated_query/kqp_federated_query_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NTestUtils {

    TString GetConnectorHost();
    ui32 GetConnectorPort();

    std::shared_ptr<NKikimr::NKqp::TKikimrRunner> MakeKikimrRunnerWithConnector();

} // namespace NTestUtils
