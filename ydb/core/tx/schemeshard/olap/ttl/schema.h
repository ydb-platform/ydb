#pragma once
#include "update.h"
#include <ydb/library/conclusion/status.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

class TOlapTTL {
private:
    NKikimrSchemeOp::TColumnDataLifeCycle Proto;
public:
    TOlapTTL() = default;
    TOlapTTL(const NKikimrSchemeOp::TColumnDataLifeCycle& proto)
        : Proto(proto)
    {

    }

    const NKikimrSchemeOp::TColumnDataLifeCycle& GetData() const {
        return Proto;
    }

    TConclusionStatus DeserializeFromProto(const NKikimrSchemeOp::TColumnDataLifeCycle& proto) {
        Proto = proto;
        return TConclusionStatus::Success();
    }

    NKikimrSchemeOp::TColumnDataLifeCycle SerializeToProto() const {
        return Proto;
    }

    TConclusionStatus Update(const TOlapTTLUpdate& update);
};

}