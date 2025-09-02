#include <ydb/core/http_proxy/http_req.h>
#include <ydb/core/testlib/test_client.h>

#include <ydb/library/testlib/service_mocks/access_service_mock.h>
#include <ydb/library/testlib/service_mocks/iam_token_service_mock.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/testing/unittest/registar.h>

#include <nlohmann/json.hpp>

using namespace NKikimr::NHttpProxy;
using namespace NKikimr::Tests;
using namespace NActors;


#include "datastreams_fixture.h"

#include "ymq_ut.h"
