#pragma once

#include "interconnect.h"
#include <ydb/library/actors/protos/interconnect.pb.h>
#include <ydb/library/actors/core/event_pb.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NActors {
    // resolve node info
    struct TEvInterconnect::TEvResolveNode: public TEventPB<TEvInterconnect::TEvResolveNode, NActorsInterconnect::TEvResolveNode, TEvInterconnect::EvResolveNode> {
        TEvResolveNode() {
        }

        TEvResolveNode(ui32 nodeId, TInstant deadline = TInstant::Max()) {
            Record.SetNodeId(nodeId);
            if (deadline != TInstant::Max()) {
                Record.SetDeadline(deadline.GetValue());
            }
        }
    };

    // node info
    struct TEvInterconnect::TEvNodeAddress: public TEventPB<TEvInterconnect::TEvNodeAddress, NActorsInterconnect::TEvNodeInfo, TEvInterconnect::EvNodeAddress> {
        TEvNodeAddress() {
        }

        TEvNodeAddress(ui32 nodeId) {
            Record.SetNodeId(nodeId);
        }
    };

    // register node
    struct TEvInterconnect::TEvRegisterNode: public TEventBase<TEvInterconnect::TEvRegisterNode, TEvInterconnect::EvRegisterNode> {
    };

    // reply on register node
    struct TEvInterconnect::TEvRegisterNodeResult: public TEventBase<TEvInterconnect::TEvRegisterNodeResult, TEvInterconnect::EvRegisterNodeResult> {
    };

    // disconnect
    struct TEvInterconnect::TEvDisconnect: public TEventLocal<TEvInterconnect::TEvDisconnect, TEvInterconnect::EvDisconnect> {
    };

}
