#include "dbg_counters.h"

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

TDBGConnectionCounters::TDBGConnectionCounters(
    NMonitoring::TDynamicCounterPtr parent)
    : ConnectAttempts(
          parent ? parent->GetCounter("ConnectAttempts", false) : nullptr)
    , ConnectOk(parent ? parent->GetCounter("ConnectOk", false) : nullptr)
    , ConnectErr(parent ? parent->GetCounter("ConnectErr", false) : nullptr)
    , Disconnects(parent ? parent->GetCounter("Disconnects", false) : nullptr)
    , Reconnects(parent ? parent->GetCounter("Reconnects", false) : nullptr)
{}

void TDBGConnectionCounters::OnConnectAttempt()
{
    if (ConnectAttempts) {
        ++*ConnectAttempts;
    }
}

void TDBGConnectionCounters::OnConnectOk()
{
    if (ConnectOk) {
        ++*ConnectOk;
    }
}

void TDBGConnectionCounters::OnConnectErr()
{
    if (ConnectErr) {
        ++*ConnectErr;
    }
}

void TDBGConnectionCounters::OnDisconnect()
{
    if (Disconnects) {
        ++*Disconnects;
    }
}

void TDBGConnectionCounters::OnReconnect()
{
    if (Reconnects) {
        ++*Reconnects;
    }
}

////////////////////////////////////////////////////////////////////////////////

TDirectBlockGroupCounters::TDirectBlockGroupCounters(
    NMonitoring::TDynamicCounterPtr parent)
    : DDisk(parent ? parent->GetSubgroup("connectionType", "DDisk") : nullptr)
    , PBuffer(
          parent ? parent->GetSubgroup("connectionType", "PBuffer") : nullptr)
{}

void TDirectBlockGroupCounters::OnConnectAttempt(
    EDBGConnectionType connectionType)
{
    Get(connectionType).OnConnectAttempt();
}

void TDirectBlockGroupCounters::OnConnectOk(EDBGConnectionType connectionType)
{
    Get(connectionType).OnConnectOk();
}

void TDirectBlockGroupCounters::OnConnectErr(EDBGConnectionType connectionType)
{
    Get(connectionType).OnConnectErr();
}

void TDirectBlockGroupCounters::OnDisconnect(EDBGConnectionType connectionType)
{
    Get(connectionType).OnDisconnect();
}

void TDirectBlockGroupCounters::OnReconnect(EDBGConnectionType connectionType)
{
    Get(connectionType).OnReconnect();
}

TDBGConnectionCounters& TDirectBlockGroupCounters::Get(
    EDBGConnectionType connectionType)
{
    switch (connectionType) {
        case EDBGConnectionType::DDisk:
            return DDisk;
        case EDBGConnectionType::PBuffer:
            return PBuffer;

        case EDBGConnectionType::MAX:
            Y_ABORT("Invalid connection type");
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
