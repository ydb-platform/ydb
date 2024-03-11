#pragma once

#include "defs.h"

#include <ydb/library/actors/interconnect/events_local.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/domain.h>
#include <ydb/core/protos/node_broker.pb.h>

/**
 * Node Broker tablet is used to allocate Node IDs for dynamic cluster nodes.
 * Nodes are registered to get ID which are leased for a limited time. Nodes
 * can extend lease time. Repeated registration of the same node will re-use
 * previously allocated ID.
 *
 * Node Broker can be used to resolve nodes by ID. Node ID lease time is never
 * reduced which allows nameservices to cache dynamic nodes info.
 *
 * IDs are allocated from a range specified by dynamic nameservice config.
 * Domains use non-overlapping ID sets (lower bits of dynamic node ID hold
 * Domain ID).
 */

namespace NKikimr {
namespace NNodeBroker {

struct TEpochInfo {
    ui64 Id = 0;
    ui64 Version = 0;
    TInstant Start;
    TInstant End;
    TInstant NextEnd;

    TEpochInfo()
        : Id(0)
        , Version(0)
    {
    }

    TEpochInfo(const NKikimrNodeBroker::TEpoch &rec)
        : Id(rec.GetId())
        , Version(rec.GetVersion())
        , Start(TInstant::FromValue(rec.GetStart()))
        , End(TInstant::FromValue(rec.GetEnd()))
        , NextEnd(TInstant::FromValue(rec.GetNextEnd()))
    {
    }

    TEpochInfo(const TEpochInfo &other) = default;
    TEpochInfo(TEpochInfo &&other) = default;

    TEpochInfo &operator=(const TEpochInfo &other) = default;
    TEpochInfo &operator=(TEpochInfo &&other) = default;

    TEpochInfo &operator=(const NKikimrNodeBroker::TEpoch &rec)
    {
        return *this = TEpochInfo(rec);
    }

    TString ToString() const
    {
        return TStringBuilder() << "#" << Id << "." << Version
                                << " " << Start << " - " << End
                                << " - " << NextEnd;
    }

    void Serialize(NKikimrNodeBroker::TEpoch &rec)
    {
        rec.SetId(Id);
        rec.SetVersion(Version);
        rec.SetStart(Start.GetValue());
        rec.SetEnd(End.GetValue());
        rec.SetNextEnd(NextEnd.GetValue());
    }
};

struct TEvNodeBroker {
    enum EEv {
        // requests
        EvListNodes = EventSpaceBegin(TKikimrEvents::ES_NODE_BROKER),
        EvResolveNode,
        EvRegistrationRequest,
        EvExtendLeaseRequest,

        // responses
        EvNodesInfo,
        EvResolvedNode,
        EvRegistrationResponse,
        EvExtendLeaseResponse,

        // config
        EvGetConfigRequest,
        EvGetConfigResponse,
        EvSetConfigRequest,
        EvSetConfigResponse,

        // TODO: remove
        // internal
        //EvNodeExpire = EvListNodes + 512,

        EvCompactTables = EvListNodes + 1024, // for tests

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_NODE_BROKER),
                  "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_NODE_BROKER)");

    template <typename TEv, typename TRecord, ui32 TEventType>
    using TEventPB = TEventShortDebugPB<TEv, TRecord, TEventType>;

    struct TEvListNodes : public TEventPB<TEvListNodes,
                                          NKikimrNodeBroker::TListNodes,
                                          EvListNodes> {
    };

    struct TEvResolveNode : public TEventPB<TEvResolveNode,
                                            NKikimrNodeBroker::TResolveNode,
                                            EvResolveNode> {
    };

    struct TEvRegistrationRequest : public TEventPB<TEvRegistrationRequest,
                                                    NKikimrNodeBroker::TRegistrationRequest,
                                                    EvRegistrationRequest> {
    };

    struct TEvExtendLeaseRequest : public TEventPB<TEvExtendLeaseRequest,
                                                   NKikimrNodeBroker::TExtendLeaseRequest,
                                                   EvExtendLeaseRequest> {
    };

    struct TEvCompactTables : public TEventLocal<TEvCompactTables, EvCompactTables> {
    };

    struct TEvNodesInfo : public TEventPreSerializedPB<TEvNodesInfo,
                                                       NKikimrNodeBroker::TNodesInfo,
                                                       EvNodesInfo> {
        TEvNodesInfo()
        {
        }

        TEvNodesInfo(const NKikimrNodeBroker::TNodesInfo &info)
            : TEventPreSerializedPB(info)
        {
        }
    };

    struct TEvResolvedNode : public TEventPB<TEvResolvedNode,
                                             NKikimrNodeBroker::TResolvedNode,
                                             EvResolvedNode> {
    };

    struct TEvRegistrationResponse : public TEventPB<TEvRegistrationResponse,
                                                     NKikimrNodeBroker::TRegistrationResponse,
                                                     EvRegistrationResponse> {
    };

    struct TEvExtendLeaseResponse : public TEventPB<TEvExtendLeaseResponse,
                                                    NKikimrNodeBroker::TExtendLeaseResponse,
                                                    EvExtendLeaseResponse> {
    };

    struct TEvGetConfigRequest : public TEventPB<TEvGetConfigRequest,
                                                 NKikimrNodeBroker::TGetConfigRequest,
                                                 EvGetConfigRequest> {
    };

    struct TEvGetConfigResponse : public TEventPB<TEvGetConfigResponse,
                                                  NKikimrNodeBroker::TGetConfigResponse,
                                                  EvGetConfigResponse> {
    };

    struct TEvSetConfigRequest : public TEventPB<TEvSetConfigRequest,
                                                 NKikimrNodeBroker::TSetConfigRequest,
                                                 EvSetConfigRequest> {
    };

    struct TEvSetConfigResponse : public TEventPB<TEvSetConfigResponse,
                                                  NKikimrNodeBroker::TSetConfigResponse,
                                                  EvSetConfigResponse> {
    };
};

constexpr ui32 DOMAIN_BITS = TDomainsInfo::DomainBits;
constexpr ui32 DOMAINS_COUNT = 1 << DOMAIN_BITS;
constexpr ui32 DOMAIN_MASK = (1 << DOMAIN_BITS) - 1;

IActor *CreateNodeBroker(const TActorId &tablet, TTabletStorageInfo *info);

} // NNodeBroker
} // NKikimr
