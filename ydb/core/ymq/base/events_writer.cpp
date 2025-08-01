#include "events_writer.h"

#include <util/stream/file.h>

#include <library/cpp/unified_agent_client/client.h>
#include <library/cpp/logger/global/global.h>

using namespace NKikimr::NSQS;

class TUaEventsWriter : public IEventsWriterWrapper {
public:
    TUaEventsWriter(const TString& uri, const NMonitoring::TDynamicCounterPtr& counters)
    {
        NUnifiedAgent::TClientParameters uaParams(uri);
        Logger.Reset(CreateDefaultLogger<TNullLog>());
        uaParams.SetLog(*Logger);
        NUnifiedAgent::TSessionParameters sessionSettings;
        sessionSettings.SetCounters(counters);
        Client = NUnifiedAgent::MakeClient(uaParams);
        Session = Client->CreateSession(sessionSettings);
    }

    void Write(const TString& data) override
    {
        Session->Send(NUnifiedAgent::TClientMessage{data, Nothing(), Nothing()});
    }

    void CloseImpl() noexcept override
    {
        Session->Close();
    }

private:
    NUnifiedAgent::TClientPtr Client;
    NUnifiedAgent::TClientSessionPtr Session;
    THolder<TLog> Logger;
};


class TFileEventsWriter : public IEventsWriterWrapper {
public:
    TFileEventsWriter(const TString& outputFileName)
        : OutputFile(TFile(outputFileName, OpenAlways | WrOnly))
        , OutStream(OutputFile)
    {}

    void Write(const TString& data) override
    {
        OutStream.Write(data);
        OutStream.Write("\n");
        OutStream.Flush();
        OutputFile.Flush();
    }
    void CloseImpl() noexcept override
    {
        OutStream.Flush();
        OutputFile.Flush();
        OutputFile.Close();
    }

private:
    TFile OutputFile;
    TFileOutput OutStream;
};


class TNullEventsWriter : public IEventsWriterWrapper {
public:
    TNullEventsWriter()
    {}

    void Write(const TString&) override
    {}

    void CloseImpl() noexcept override
    {}
};



IEventsWriterWrapper::TPtr TSqsEventsWriterFactory::CreateEventsWriter(const NKikimrConfig::TSqsConfig& config,  const NMonitoring::TDynamicCounterPtr& counters) const {
    const auto& ycSearchCfg = config.GetYcSearchEventsConfig();
    switch (ycSearchCfg.OutputMethod_case()) {
        case NKikimrConfig::TSqsConfig::TYcSearchEventsConfig::kUnifiedAgentUri:
            return new TUaEventsWriter(ycSearchCfg.GetUnifiedAgentUri(), counters);
        case NKikimrConfig::TSqsConfig::TYcSearchEventsConfig::kOutputFileName:
            return new TFileEventsWriter(ycSearchCfg.GetOutputFileName());
        default:
            return new TNullEventsWriter();
    }
    return new TNullEventsWriter();
}

IEventsWriterWrapper::TPtr TSqsEventsWriterFactory::CreateCloudEventsWriter(const NKikimrConfig::TSqsConfig& config, const NMonitoring::TDynamicCounterPtr& counters) const {
    const auto& cloudEventsCfg = config.GetCloudEventsConfig();
    if (cloudEventsCfg.HasUnifiedAgentUri()) {
        return new TUaEventsWriter(cloudEventsCfg.GetUnifiedAgentUri(), counters);
    } else {
        return new TNullEventsWriter();
    }
}
