#include <ydb/tests/fq/pq_async_io/ut_helpers.h>

#include <ydb/library/yql/utils/yql_panic.h>

#include <library/cpp/testing/unittest/registar.h>

#include <thread>

namespace NYql::NDq {


Y_UNIT_TEST_SUITE(TDqPqRdReadActorTest) {
    Y_UNIT_TEST_F(TestReadFromTopic, TPqIoTestFixture) {

        InitRdSource(BuildPqTopicSourceSettings("topicName"));

    }
}
} // NYql::NDq
