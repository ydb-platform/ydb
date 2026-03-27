#pragma once

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/unified_agent_client/client.h>

#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/stream/file.h>

namespace NKikimr::NPQ::NCloudEvents {

class IEventsWriter {
public:
    using TPtr = THolder<IEventsWriter>;
    virtual ~IEventsWriter() = default;
    virtual void Write(const TString& data) = 0;
};

/// Writes cloud events to a local file in audit-compatible format: timestamp + cloud_event_json wrapper per line.
class TFileEventsWriter : public IEventsWriter {
public:
    explicit TFileEventsWriter(const TString& filePath);
    void Write(const TString& data) override;

private:
    TFile OutputFile;
    TFileOutput OutStream;
};

class TUaEventsWriter : public IEventsWriter {
public:
    TUaEventsWriter(const TString& uri, const NMonitoring::TDynamicCounterPtr& counters);
    void Write(const TString& data);
    void Close();
    ~TUaEventsWriter();

private:
    NUnifiedAgent::TClientPtr Client;
    NUnifiedAgent::TClientSessionPtr Session;
    THolder<TLog> Logger;
    bool Closed = false;
};

} // namespace NKikimr::NPQ::NCloudEvents
