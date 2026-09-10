#pragma once

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

// Type of a connection a DirectBlockGroup keeps with a host.
enum class EDBGConnectionType
{
    DDisk,
    PBuffer,

    MAX
};

////////////////////////////////////////////////////////////////////////////////

// Per-connection-type session counters for a DirectBlockGroup.
class TDBGConnectionCounters
{
private:
    // Number of connection attempts (each DoEstablishConnection call).
    NMonitoring::TDynamicCounters::TCounterPtr ConnectAttempts;
    // Number of successfully established connections/sessions.
    NMonitoring::TDynamicCounters::TCounterPtr ConnectOk;
    // Number of failed connection attempts.
    NMonitoring::TDynamicCounters::TCounterPtr ConnectErr;
    // Number of node disconnects (OnNodeDisconnected).
    NMonitoring::TDynamicCounters::TCounterPtr Disconnects;
    // Number of scheduled reconnects (ReEstablishConnection).
    NMonitoring::TDynamicCounters::TCounterPtr Reconnects;

public:
    explicit TDBGConnectionCounters(NMonitoring::TDynamicCounterPtr parent);

    void OnConnectAttempt();
    void OnConnectOk();
    void OnConnectErr();
    void OnDisconnect();
    void OnReconnect();
};

////////////////////////////////////////////////////////////////////////////////

// Aggregates session counters for a DirectBlockGroup, split by connection type.
class TDirectBlockGroupCounters
{
private:
    TDBGConnectionCounters DDisk;
    TDBGConnectionCounters PBuffer;

public:
    explicit TDirectBlockGroupCounters(NMonitoring::TDynamicCounterPtr parent);

    void OnConnectAttempt(EDBGConnectionType connectionType);
    void OnConnectOk(EDBGConnectionType connectionType);
    void OnConnectErr(EDBGConnectionType connectionType);
    void OnDisconnect(EDBGConnectionType connectionType);
    void OnReconnect(EDBGConnectionType connectionType);

private:
    TDBGConnectionCounters& Get(EDBGConnectionType connectionType);
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
