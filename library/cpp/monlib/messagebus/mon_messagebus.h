#pragma once

#include <library/cpp/messagebus/ybus.h>
#include <library/cpp/messagebus/www/www.h>

#include <library/cpp/monlib/service/pages/mon_page.h>

namespace NMonitoring {
    template <class TBusSmth>
    class TBusSmthMonPage: public NMonitoring::IMonPage {
    private:
        TBusSmth* Smth;

    public:
        explicit TBusSmthMonPage(const TString& name, const TString& title, TBusSmth* smth)
            : IMonPage("msgbus/" + name, title)
            , Smth(smth)
        {
        }
        void Output(NMonitoring::IMonHttpRequest& request) override {
            Y_UNUSED(request);
            request.Output() << NMonitoring::HTTPOKHTML;
            request.Output() << "<h2>" << Title << "</h2>";
            request.Output() << "<pre>";
            request.Output() << Smth->GetStatus();
            request.Output() << "</pre>";
        }
    };

    using TBusQueueMonPage = TBusSmthMonPage<NBus::TBusMessageQueue>;
    using TBusModuleMonPage = TBusSmthMonPage<NBus::TBusModule>;

    class TBusNgMonPage: public NMonitoring::IMonPage {
    public:
        TIntrusivePtr<NBus::TBusWww> BusWww;

    public:
        TBusNgMonPage()
            : IMonPage("messagebus", "MessageBus")
            , BusWww(new NBus::TBusWww)
        {
        }
        void Output(NMonitoring::IMonHttpRequest& request) override;
    };

}
