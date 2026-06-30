#include "events_writer.h"
#include <library/cpp/json/json_value.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/logger/global/global.h>
#include <library/cpp/yt/coding/varint.h>

namespace NKikimr::NPQ::NCloudEvents {

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
    Session = Client->CreateSession(sessionSettings);
}

void TUaEventsWriter::Write(const TString& data) {
    Session->Send(NUnifiedAgent::TClientMessage{data, Nothing(), Nothing()});
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
