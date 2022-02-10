#include "golovan_page.h"

#include <library/cpp/monlib/service/pages/templates.h>

#include <util/string/split.h>
#include <util/system/tls.h>

using namespace NMonitoring;

class TGolovanCountableConsumer: public ICountableConsumer {
public:
    using TOutputCallback = std::function<void()>;

    TGolovanCountableConsumer(IOutputStream& out, TOutputCallback& OutputCallback)
        : out(out)
    {
        if (OutputCallback) {
            OutputCallback();
        }

        out << HTTPOKJSON << "[";
        FirstCounter = true;
    }

    void OnCounter(const TString&, const TString& value, const TCounterForPtr* counter) override {
        if (FirstCounter) {
            FirstCounter = false;
        } else {
            out << ",";
        }

        out << "[\"" << prefix + value;
        if (counter->ForDerivative()) {
            out << "_dmmm";
        } else {
            out << "_ahhh";
        }

        out << "\"," << counter->Val() << "]";
    }

    void OnHistogram(const TString&, const TString&, IHistogramSnapshotPtr, bool) override {
    }

    void OnGroupBegin(const TString&, const TString& value, const TDynamicCounters*) override {
        prefix += value;
        if (!value.empty()) {
            prefix += "_";
        }
    }

    void OnGroupEnd(const TString&, const TString&, const TDynamicCounters*) override {
        prefix = "";
    }

    void Flush() {
        out << "]";
        out.Flush();
    }

private:
    IOutputStream& out;
    bool FirstCounter;
    TString prefix;
};

TGolovanCountersPage::TGolovanCountersPage(const TString& path, TIntrusivePtr<NMonitoring::TDynamicCounters> counters,
                                           TOutputCallback outputCallback)
    : IMonPage(path)
    , Counters(counters)
    , OutputCallback(outputCallback)
{
}

void TGolovanCountersPage::Output(IMonHttpRequest& request) {
    TGolovanCountableConsumer consumer(request.Output(), OutputCallback);
    Counters->Accept("", "", consumer);
    consumer.Flush();
}
