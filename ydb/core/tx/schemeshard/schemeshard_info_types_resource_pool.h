#pragma once

#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <util/generic/ptr.h>

namespace NKikimr {
namespace NSchemeShard {

struct TResourcePoolInfo : TSimpleRefCount<TResourcePoolInfo> {
    using TPtr = TIntrusivePtr<TResourcePoolInfo>;

    ui64 AlterVersion = 0;
    NKikimrSchemeOp::TResourcePoolProperties Properties;
};

}
}
