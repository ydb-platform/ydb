#include "events_writer.h"
#include <library/cpp/json/json_value.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/logger/global/global.h>
#include <library/cpp/yt/coding/varint.h>

namespace NKikimr::NPQ::NCloudEvents {

namespace {

class TUaEventsSession final : public IUaEventsSession {
public:
    explicit TUaEventsSession(NUnifiedAgent::TClientSessionPtr session)
        : Session(std::move(session))
    {}

    void Send(const TString& data) override {
        Session->Send(NUnifiedAgent::TClientMessage{data, Nothing(), Nothing()});
    }

    void Close() override {
        Session->Close();
    }

private:
    NUnifiedAgent::TClientSessionPtr Session;
};

} // anonymous namespace

TFileEventsWriter::TFileEventsWriter(const TString& filePath)
    : OutputFile(TFile(filePath, OpenAlways | WrOnly | ForAppend))
    , OutStream(OutputFile) {}

void TFileEventsWriter::Write(const TString& data) {
    NYT::WriteVarUint64(&OutStream, data.size());
    OutStream.Write(data);
    OutStream.Flush();
    OutputFile.Flush();
}

TUaEventsWriter::TUaEventsWriter(const TString& uri, const NMonitoring::TDynamicCounterPtr& counters) {
    NUnifiedAgent::TClientParameters uaParams(uri);
    Logger.Reset(CreateDefaultLogger<TNullLog>());
    uaParams.SetLog(*Logger);
    NUnifiedAgent::TSessionParameters sessionSettings;
    sessionSettings.SetCounters(counters);
    Client = NUnifiedAgent::MakeClient(uaParams);
    Session = MakeHolder<TUaEventsSession>(Client->CreateSession(sessionSettings));
}

TUaEventsWriter::TUaEventsWriter(IUaEventsSession::TPtr session)
    : Session(std::move(session))
{}

void TUaEventsWriter::Write(const TString& data) {
    Session->Send(data);
}

void TUaEventsWriter::Close() {
    if (std::exchange(Closed, true)) {
        return;
    }

    Session->Close();
}

TUaEventsWriter::~TUaEventsWriter() {
    Close();
}

} // namespace NKikimr::NPQ::NCloudEvents
