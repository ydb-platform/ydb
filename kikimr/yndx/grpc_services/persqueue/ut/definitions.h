#pragma once
#include <kikimr/persqueue/sdk/deprecated/cpp/v2/ut_utils/data_writer.h>
#include <kikimr/persqueue/sdk/deprecated/cpp/v2/ut_utils/test_server.h>

namespace NKikimr::NPersQueueTests {


namespace {
    const TString DEFAULT_TOPIC_NAME = "rt3.dc1--topic1";
    const TString FC_TOPIC_PATH = "/Root/account1/root-acc-topic";
    const TString SHORT_TOPIC_NAME = "topic1";
}

NPersQueue::NTests::TPQDataWriter MakeDataWriter(NPersQueue::TTestServer& server, const TString& srcId = "source") {
    return NPersQueue::NTests::TPQDataWriter(DEFAULT_TOPIC_NAME, SHORT_TOPIC_NAME, srcId, server);
}

} // namespace NKikimr::NPersQueueTests
