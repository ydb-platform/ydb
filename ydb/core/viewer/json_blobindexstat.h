#pragma once
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/interconnect.h>
#include <library/cpp/actors/core/mon.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/util/tuples.h>
#include "json_vdisk_req.h"

namespace NKikimr {
namespace NViewer {

using TJsonBlobIndexStat = TJsonVDiskRequest<TEvGetLogoBlobIndexStatRequest, TEvGetLogoBlobIndexStatResponse>;

template <>
struct TJsonRequestSummary<TJsonBlobIndexStat> {
    static TString GetSummary() {
        return "\"Get logoblob index stat from VDisk\"";
    }
};

template <>
struct TJsonRequestDescription<TJsonBlobIndexStat> {
    static TString GetDescription() {
        return "\"Get logoblob index stat from VDisk\"";
    }
};

}
}
