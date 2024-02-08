#pragma once

#include "defs.h"

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <ydb/core/base/domain.h>
#include <ydb/core/protos/node_broker.pb.h>

namespace NKikimrConfig {
    class TStaticNameserviceConfig;
} // NKikimrConfig

namespace NKikimr {
namespace NNodeBroker {

// Create nameservice for static node.
IActor *CreateDynamicNameserver(const TIntrusivePtr<TTableNameserverSetup> &setup,
                                ui32 poolId = 0);

// Create nameservice for dynamic node providing its info.
IActor *CreateDynamicNameserver(const TIntrusivePtr<TTableNameserverSetup> &setup,
                                const NKikimrNodeBroker::TNodeInfo &node,
                                const TDomainsInfo &domains,
                                ui32 poolId = 0);

TIntrusivePtr<TTableNameserverSetup> BuildNameserverTable(const NKikimrConfig::TStaticNameserviceConfig& nsConfig);

} // NNodeBroker
} // NKikimr
