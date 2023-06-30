#include "requests.h"

namespace NYT::NZookeeper {

////////////////////////////////////////////////////////////////////////////////

void TReqStartSession::Deserialize(IZookeeperProtocolReader* reader)
{
    ProtocolVersion = reader->ReadInt();
    LastZxidSeen = reader->ReadLong();
    Timeout = TDuration::MilliSeconds(reader->ReadInt());
    SessionId = reader->ReadLong();
    Password = reader->ReadString();
    ReadOnly = reader->ReadBool();
}

void TRspStartSession::Serialize(IZookeeperProtocolWriter* writer) const
{
    writer->WriteInt(ProtocolVersion);
    writer->WriteInt(Timeout.MilliSeconds());
    writer->WriteLong(SessionId);
    writer->WriteString(Password);
    writer->WriteBool(ReadOnly);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NZookeeper
