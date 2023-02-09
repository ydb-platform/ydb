#include "counters.h"

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/http/io/stream.h>
#include <library/cpp/json/json_reader.h>
#include <util/string/builder.h>
#include <util/string/vector.h>
#include <util/string/join.h>
#include <util/network/socket.h>

namespace NKikimr::NPersQueueTests {

NJson::TJsonValue SendQuery(ui16 port, const TString& query, bool mayFail) {
    Cerr << "===Request counters with query: " << query << Endl;
    TNetworkAddress addr("localhost", port);
    TSocket s(addr);
    SendMinimalHttpRequest(s, "localhost", query);
    TSocketInput si(s);
    THttpInput input(&si);
    TString firstLine = input.FirstLine();

    const auto httpCode = ParseHttpRetCode(firstLine);
    NJson::TJsonValue value;
    bool res = NJson::ReadJsonTree(&input, &value);

    Cerr << "counters: " << value.GetStringRobust() << "\n";

    UNIT_ASSERT(res);
    if (mayFail && httpCode != 200u) {
        return {};
    } else {
        UNIT_ASSERT_VALUES_EQUAL(httpCode, 200u);
    }

    return value;
}


NJson::TJsonValue GetCountersLegacy(ui16 port, const TString& counters, const TString& subsystem,
                                    const TString& topicPath, const TString& clientDc,
                                    const TString& originalDc, const TString& client,
                                    const TString& consumerPath) {
    TString dcFilter = "";
    {
        if (originalDc) {
            dcFilter = TStringBuilder() << "/OriginDC=" << originalDc;
        } else if (clientDc) {
            dcFilter = TStringBuilder() << "/ClientDC=" << clientDc;
        }
    }
    TVector<TString> pathItems = SplitString(topicPath, "/");
    UNIT_ASSERT(pathItems.size() >= 2);
    TString account = pathItems.front();
    TString producer = JoinRange("@", pathItems.begin(), pathItems.end() - 1);
    TStringBuilder queryBuilder = TStringBuilder() <<
        "/counters/counters=" << counters <<
        "/subsystem=" << subsystem <<
        "/Account=" << account <<
        "/Producer=" << producer <<
        "/Topic=" << Join("--", producer, pathItems.back()) <<
        "/TopicPath=" << JoinRange("%2F", pathItems.begin(), pathItems.end()) <<
        dcFilter;

    if (consumerPath) {
        auto consumerPathItems = SplitString(consumerPath, "/");
        queryBuilder <<
            "/Client=" << client <<
            "/ConsumerPath=" << JoinRange("%2F", consumerPathItems.begin(), consumerPathItems.end());
    }
    queryBuilder << "/json";

    return SendQuery(port, queryBuilder);
}

NJson::TJsonValue GetClientCountersLegacy(ui16 port, const TString& counters, const TString& subsystem,
                                          const TString& client, const TString& consumerPath) {
    TVector<TString> consumerPathItems = SplitString(consumerPath, "/");
    UNIT_ASSERT(consumerPathItems.size() >= 2);
    TStringBuilder queryBuilder = TStringBuilder() <<
        "/counters/counters=" << counters <<
        "/subsystem=" << subsystem <<
        "/Client=" << client <<
        "/ConsumerPath=" << JoinRange("%2F", consumerPathItems.begin(), consumerPathItems.end()) <<
        "/json";

    return SendQuery(port, queryBuilder);
}

NJson::TJsonValue GetCounters1stClass(ui16 port, const TString& counters,
                                      const TString& databasePath,
                                      const TString& cloudId, const TString& databaseId,
                                      const TString& folderId, const TString& topicName,
                                      const TString& consumer, const TString& host,
                                      const TString& partition) {
    bool mayFail = false;
    TVector<TString> pathItems = SplitString(topicName, "/");
    TStringBuilder queryBuilder;
    queryBuilder <<
        "/counters/counters=" << counters <<
        "/database=" << databasePath <<
        "/cloud_id=" << cloudId <<
        "/folder_id=" << folderId <<
        "/database_id=" << databaseId <<
        "/topic=" << JoinRange("%2F", pathItems.begin(), pathItems.end());

    if (consumer) {
        queryBuilder <<
            "/consumer=" << consumer;
    }

    if (host) {
        queryBuilder <<
            "/host=" << host;
    }

    if (partition) {
        queryBuilder <<
            "/partition=" << partition;
        mayFail = true;
    }

    queryBuilder << "/json";

    return SendQuery(port, queryBuilder, mayFail);
}

} // NKikimr::NPersQueueTests
