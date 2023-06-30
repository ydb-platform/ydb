#pragma once

#include "command.h"

#include <yt/yt/client/chaos_client/replication_card.h>

#include <yt/yt/core/ytree/yson_serializable.h>

namespace NYT::NDriver {

////////////////////////////////////////////////////////////////////////////////

class TUpdateChaosTableReplicaProgressCommand
    : public TTypedCommand<NApi::TUpdateChaosTableReplicaProgressOptions>
{
public:
    TUpdateChaosTableReplicaProgressCommand();

private:
    NChaosClient::TReplicaId ReplicaId;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TAlterReplicationCardCommand
    : public TTypedCommand<NApi::TAlterReplicationCardOptions>
{
public:
    TAlterReplicationCardCommand();

private:
    NChaosClient::TReplicationCardId ReplicationCardId;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
