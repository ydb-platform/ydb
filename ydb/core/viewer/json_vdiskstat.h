#pragma once
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/interconnect.h>
#include <library/cpp/actors/core/mon.h>
#include <ydb/core/protos/services.pb.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/util/tuples.h>
#include "json_vdisk_req.h"

namespace NKikimr {
namespace NViewer {

using TJsonVDiskStat = TJsonVDiskRequest<TEvVDiskStatRequest, TEvVDiskStatResponse>;

template <>
struct TJsonRequestSummary<TJsonVDiskStat> {
    static TString GetSummary() {
        return "\"VDisk statistic\"";
    }
};

template <>
struct TJsonRequestDescription<TJsonVDiskStat> {
    static TString GetDescription() {
        return "\"VDisk statistic\"";
    }
};

}
}
