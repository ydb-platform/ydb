#include "runtime.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/mind/dynamic_nameserver.h>
#include <ydb/library/actors/dnsresolver/dnsresolver.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <ydb/library/actors/interconnect/interconnect_tcp_server.h>
#include <util/generic/xrange.h>

namespace NActors {

    TTestBasicRuntime::~TTestBasicRuntime()
    {

    }

    void TTestBasicRuntime::Initialize(TEgg egg)
    {
        AddICStuff();
        AddAuditLogStuff();

        TTestActorRuntime::Initialize(std::move(egg));
    }

    void TTestBasicRuntime::AddICStuff()
    {
        TIntrusivePtr<TTableNameserverSetup> table = new TTableNameserverSetup;

        for (ui32 nodeIndex = 0; nodeIndex < GetNodeCount(); ++nodeIndex) {
            const ui16 port = 12001 + nodeIndex;
            table->StaticNodeTable[FirstNodeId + nodeIndex] =
                std::pair<TString, ui32>("::1", UseRealInterconnect ? GetPortManager().GetPort(port) : port);

            NActorsInterconnect::TNodeLocation proto;
            proto.SetDataCenter(ToString(nodeIndex % DataCenterCount + 1));
            proto.SetModule(ToString(nodeIndex + 1));
            proto.SetRack(ToString(nodeIndex + 1));
            proto.SetUnit(ToString(nodeIndex + 1));
            table->StaticNodeTable[FirstNodeId + nodeIndex].Location = LocationCallback
                ? LocationCallback(nodeIndex)
                : TNodeLocation(proto);
        }

        const TActorId dnsId = NDnsResolver::MakeDnsResolverActorId();
        const TActorId namesId = GetNameserviceActorId();
        for (auto num : xrange(GetNodeCount())) {
            auto* node = GetRawNode(num);

            node->Poller.Reset(new NInterconnect::TPollerThreads);
            node->Poller->Start();

            AddLocalService(dnsId,
                TActorSetupCmd(NDnsResolver::CreateOnDemandDnsResolver(),
                               TMailboxType::Simple, 0), num);

            AddLocalService(namesId,
                TActorSetupCmd(NKikimr::NNodeBroker::CreateDynamicNameserver(table),
                               TMailboxType::Simple, 0), num);

            const auto& nameNode = table->StaticNodeTable[FirstNodeId + num];

            TIntrusivePtr<TInterconnectProxyCommon> common;
            common.Reset(new TInterconnectProxyCommon);
            common->NameserviceId = namesId;
            common->TechnicalSelfHostName = "::1";
            common->ClusterUUID = ClusterUUID;
            common->AcceptUUID = {ClusterUUID};

            if (ICCommonSetupper) {
                ICCommonSetupper(num, common);
            }

            if (UseRealInterconnect) {
                auto listener = new TInterconnectListenerTCP(nameNode.first, nameNode.second, common);
                AddLocalService({}, TActorSetupCmd(listener, TMailboxType::Simple, InterconnectPoolId()), num);
                AddLocalService(MakePollerActorId(), TActorSetupCmd(CreatePollerActor(), TMailboxType::Simple, 0), num);
            }
        }
    }

    void TTestBasicRuntime::AddAuditLogStuff()
    {
        if (AuditLogBackends) {
            for (ui32 nodeIndex = 0; nodeIndex < GetNodeCount(); ++nodeIndex) {
                AddLocalService(
                    NKikimr::NAudit::MakeAuditServiceID(),
                    TActorSetupCmd(
                        NKikimr::NAudit::CreateAuditWriter(std::move(AuditLogBackends)).Release(),
                        TMailboxType::HTSwap,
                        0
                    ),
                    nodeIndex
                );
            }
        }
    }
}
