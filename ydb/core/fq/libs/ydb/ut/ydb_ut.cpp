#include <ydb/core/fq/libs/ydb/ydb.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NFq {

Y_UNIT_TEST_SUITE(TFqYdbTest) {

    Y_UNIT_TEST(ShouldStatusToIssuesProcessExceptions)
    {
        auto promise = NThreading::NewPromise<NYdb::TStatus>();
        auto future = promise.GetFuture();
        TString text("Test exception");
        promise.SetException(text);
        NThreading::TFuture<NYql::TIssues> future2 = NFq::StatusToIssues(future);

        NYql::TIssues issues = future2.GetValueSync();
        UNIT_ASSERT(issues.Size() == 1);
        UNIT_ASSERT(issues.ToString().Contains(text));
    }
}

} // namespace NFq
