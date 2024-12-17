#pragma once
#include "common.h"
#include <ydb/core/tx/columnshard/bg_tasks/protos/data.pb.h>
#include <ydb/core/tx/columnshard/bg_tasks/abstract/control.h>

namespace NKikimr::NOlap::NBackground {

struct TEvSessionControl: public TEventPB<TEvSessionControl, NKikimrTxBackgroundProto::TSessionControlContainer, TEvents::EvSessionControl> {
    TEvSessionControl() = default;

    TEvSessionControl(const TSessionControlContainer& container) {
        Record = container.SerializeToProto();
    }
};

}