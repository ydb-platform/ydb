#pragma once

#include "defs.h"
#include "console.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/interconnect/interconnect.h> 

namespace NKikimr::NConsole {

class TConfigurationInfoCollector : public TActorBootstrapped<TConfigurationInfoCollector> {
private:
    struct TEndpoint {
        TString Host;
        ui16 Port;
    };

    using TBase = TActorBootstrapped<TConfigurationInfoCollector>;
    const TActorId ReplyToActorId;

    ui32 V1Nodes = 0;
    ui32 V2Nodes = 0;
    ui32 UnknownNodes = 0;
    std::vector<ui32> V1NodesList;
    std::vector<ui32> V2NodesList;
    std::vector<ui32> UnknownNodesList;
    bool ListNodes = false;

    std::unordered_set<ui32> PendingNodes;
    std::unordered_map<ui32, TEndpoint> NodesInfo;
    ui32 TotalNodes = 0;

    const TDuration Timeout = TDuration::Seconds(5);

    struct TEvPrivate {
        enum EEv {
            EvTimeout = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
            EvEnd
        };
        static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_PRIVATE)");

        struct TEvTimeout : public TEventLocal<TEvTimeout, EvTimeout> {};
    };

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::CMS_CONFIG_INFO_COLLECTOR;
    }

    TConfigurationInfoCollector(TActorId replyToActorId, bool listNodes);

    void Bootstrap();

private:
    void Handle(TEvInterconnect::TEvNodesInfo::TPtr &ev);
    void Handle(TEvConsole::TEvGetNodeConfigurationVersionResponse::TPtr &ev);
    void Handle(TEvPrivate::TEvTimeout::TPtr &ev);

    void RequestNodeList();
    void RequestNodeVersions();
    void ReplyAndDie();

    STFUNC(StateWork);
};

IActor *CreateConfigurationInfoCollector(TActorId replyToActorId, bool listNodes);

} // namespace NKikimr::NConsole
