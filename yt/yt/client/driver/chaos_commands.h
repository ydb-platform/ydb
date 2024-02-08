#pragma once

#include "command.h"

#include <yt/yt/client/chaos_client/replication_card.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NDriver {

////////////////////////////////////////////////////////////////////////////////

class TUpdateChaosTableReplicaProgressCommand
    : public TTypedCommand<NApi::TUpdateChaosTableReplicaProgressOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TUpdateChaosTableReplicaProgressCommand);

    static void Register(TRegistrar registrar);

private:
    NChaosClient::TReplicaId ReplicaId;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TAlterReplicationCardCommand
    : public TTypedCommand<NApi::TAlterReplicationCardOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TAlterReplicationCardCommand);

    static void Register(TRegistrar registrar);

private:
    NChaosClient::TReplicationCardId ReplicationCardId;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
