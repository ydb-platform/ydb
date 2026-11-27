#include "logging.h"

#include <library/cpp/logger/null.h>
#include <library/cpp/logger/stream.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/string.h>
#include <util/stream/str.h>
#include <util/system/event.h>

namespace NYdb::NBS {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TSync: TNullLogBackend
{
    TAutoEvent Event;

    void WriteData(const TLogRecord&) override
    {
        Event.Signal();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TAsyncLoggerTest)
{
    Y_UNIT_TEST(TestEnqueueOrder)
    {
        TString result;
        TStringOutput so(result);

        auto sync = std::make_shared<TSync>();
        auto backend = std::make_shared<TStreamLogBackend>(&so);

        auto logger = CreateAsyncLogger();
        logger->Start();

        UNIT_ASSERT(result.empty());

        logger->Enqueue(backend, TLOG_INFO, "Line 1\n");
        logger->Enqueue(backend, TLOG_INFO, "Line 2\n");
        logger->Enqueue(backend, TLOG_INFO, "Line 3\n");
        logger->Enqueue(sync, TLOG_INFO, "");

        sync->Event.Wait();
        UNIT_ASSERT_STRINGS_EQUAL(result, "Line 1\nLine 2\nLine 3\n");
    }
}

}   // namespace NYdb::NBS
