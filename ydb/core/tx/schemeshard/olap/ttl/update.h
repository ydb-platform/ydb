#pragma once
#include <ydb/core/protos/flat_scheme_op.pb.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

class TOlapTTLUpdate {
private:
    NKikimrSchemeOp::TColumnDataLifeCycle Proto;
public:
    TOlapTTLUpdate(const NKikimrSchemeOp::TColumnDataLifeCycle& proto)
        : Proto(proto)
    {

    }

    const NKikimrSchemeOp::TColumnDataLifeCycle& GetPatch() const {
        return Proto;
    }
};

}