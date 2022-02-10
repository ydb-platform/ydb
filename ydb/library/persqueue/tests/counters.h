#pragma once

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/http/io/stream.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_value.h>

#include <util/string/builder.h>
#include <util/string/vector.h>
#include <util/string/join.h>
#include <util/network/socket.h>



namespace NKikimr::NPersQueueTests {

NJson::TJsonValue GetCounters(ui16 port, const TString& counters, const TString& subsystem, const TString& topicPath, const TString& clientDc = "", const TString& originalDc = "") {
    TString dcFilter = "";
    {
        auto prepareDC = [](TString dc) {
            dc.front() = std::toupper(dc.front());
            return dc;
        };
        if (originalDc) {
            dcFilter = TStringBuilder() << "/OriginDC=" << prepareDC(originalDc);
        } else if (clientDc) {
            dcFilter = TStringBuilder() << "/ClientDC=" << prepareDC(clientDc);
        }
    }
    TVector<TString> pathItems = SplitString(topicPath, "/");
    UNIT_ASSERT(pathItems.size() >= 2);
    TString account = pathItems.front();
    TString producer = JoinRange("@", pathItems.begin(), pathItems.end() - 1);
    TString query = TStringBuilder() << "/counters/counters=" << counters
        << "/subsystem=" << subsystem << "/Account=" << account << "/Producer=" << producer
        << "/Topic=" << Join("--", producer, pathItems.back()) << "/TopicPath=" << JoinRange("%2F", pathItems.begin(), pathItems.end())
        << dcFilter << "/json";
    
    
    TNetworkAddress addr("localhost", port);
    TString q2 = TStringBuilder() << "/counters/counters=" << counters
        << "/subsystem=" << subsystem;// << "/Account=" << account << "/Producer=" << producer;
    //q2 += "/json";
    
    /*Cerr << "q2: " << q2 << Endl;
    TSocket s2(addr);
    SendMinimalHttpRequest(s2, "localhost", q2);
    TSocketInput si2(s2);
    THttpInput input2(&si2);
    Cerr << "===Counters response: " << input2.ReadAll() << Endl;*/
    
    
    
    Cerr << "===Request counters with query: " << query << Endl;
    TSocket s(addr);
    SendMinimalHttpRequest(s, "localhost", query);
    TSocketInput si(s);
    THttpInput input(&si);
    TString firstLine = input.FirstLine();
    //Cerr << "Counters2: '" << firstLine << "' content: "  << input.ReadAll() << Endl;
    
    unsigned httpCode = ParseHttpRetCode(firstLine);
    UNIT_ASSERT_VALUES_EQUAL(httpCode, 200u);
    NJson::TJsonValue value;
    UNIT_ASSERT(NJson::ReadJsonTree(&input, &value));

    Cerr << "counters: " << value.GetStringRobust() << "\n";
    return value;
}

} // NKikimr::NPersQueueTests
