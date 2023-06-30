#pragma once

#include "public.h"

#include "protocol.h"

namespace NYT::NZookeeper {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ERequestType,
    ((None)               (-1))
);

////////////////////////////////////////////////////////////////////////////////

struct TReqStartSession
{
    int ProtocolVersion = -1;
    i64 LastZxidSeen = -1;
    TDuration Timeout = TDuration::Zero();
    i64 SessionId = -1;
    TString Password;
    bool ReadOnly = false;

    void Deserialize(IZookeeperProtocolReader* reader);
};

struct TRspStartSession
{
    int ProtocolVersion = -1;
    TDuration Timeout = TDuration::Zero();
    i64 SessionId = -1;
    TString Password;
    bool ReadOnly = false;

    void Serialize(IZookeeperProtocolWriter* writer) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NZookeeper
