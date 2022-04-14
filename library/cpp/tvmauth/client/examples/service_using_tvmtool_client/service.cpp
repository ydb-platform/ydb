#include "service.h"

#include <library/cpp/tvmauth/client/facade.h>

#include <library/cpp/cgiparam/cgiparam.h>
#include <library/cpp/http/server/response.h>
#include <library/cpp/http/simple/http_client.h>
#include <library/cpp/json/json_reader.h>

namespace NExample {
    static const TString BACK_C = "BACK_C";

    TSomeService::TSomeService(const TConfig& cfg)
        : Config_(cfg)
    {
        NTvmAuth::TLoggerPtr log = MakeIntrusive<NTvmAuth::TCerrLogger>(7);

        Tvm_ = MakeHolder<NTvmAuth::TTvmClient>(
            NTvmAuth::NTvmTool::TClientSettings(
                "my_service" // specified in Qloud/YP/tvmtool interface
                ),
            log);
    }

    TSomeService::~TSomeService() {
    }

    void TSomeService::HandleRequest(THttpInput& in, THttpOutput& out) {
        auto servIt = std::find_if(in.Headers().Begin(),
                                   in.Headers().End(),
                                   [](const auto& h) { return h.Name() == "X-Ya-Service-Ticket"; });
        auto userIt = std::find_if(in.Headers().Begin(),
                                   in.Headers().End(),
                                   [](const auto& h) { return h.Name() == "X-Ya-User-Ticket"; });
        try {
            if (servIt == in.Headers().End() || userIt == in.Headers().End()) {
                ythrow yexception() << "Need tickets";
            }

            // WARNING: См. Здесь
            NTvmAuth::TCheckedServiceTicket st = Tvm_->CheckServiceTicket(servIt->Value());
            NTvmAuth::TCheckedUserTicket ut = Tvm_->CheckUserTicket(userIt->Value());
            if (!st || !ut) {
                ythrow yexception() << "Invalid tickets";
            }

            // WARNING: См. Здесь
            // Ждём ABC - после их релиза эти три строки можно будет удалить
            if (Config_.AllowedTvmIds.find(st.GetSrc()) == Config_.AllowedTvmIds.end()) {
                ythrow yexception() << "Consumer is not allowed";
            }

            // WARNING: См. Здесь
            if (!ut.HasScope("some_service:allow_secret_data")) {
                ythrow yexception() << "UserTicket does not have scopes for secret data";
            }

            // Access-log
            Cout << "Data fetched for: " << ut.GetDefaultUid() << Endl;

            THttpResponse resp(HTTP_OK);
            resp.SetContent(GetDataFromBackendC(userIt->Value()), "text/plain");
            resp.OutTo(out);
        } catch (...) {
            THttpResponse resp(HTTP_BAD_REQUEST);
            resp.SetContent("Request can not be performed", "text/plain");
            resp.OutTo(out);
        }

        out.Finish();
    }

    TString TSomeService::GetDataFromBackendC(const TString& userTicket) {
        TSimpleHttpClient cl("my_backend", // specified in Qloud/YP/tvmtool interface
                             80);
        TStringStream s;
        cl.DoGet("/api?",
                 &s,
                 // WARNING: См. Здесь
                 {{"X-Ya-Service-Ticket", Tvm_->GetServiceTicketFor(BACK_C)},
                  {"X-Ya-User-Ticket", userTicket}});
        return s.Str();
    }
}
