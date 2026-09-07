#pragma once

#include "schemeshard_info_types_base.h"

namespace NKikimr {
namespace NSchemeShard {

struct TResourcePoolInfo : TSimpleRefCount<TResourcePoolInfo> {
    using TPtr = TIntrusivePtr<TResourcePoolInfo>;

    ui64 AlterVersion = 0;
    NKikimrSchemeOp::TResourcePoolProperties Properties;
};

}
}
