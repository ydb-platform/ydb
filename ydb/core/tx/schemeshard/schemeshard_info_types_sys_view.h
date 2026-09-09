#pragma once

#include "schemeshard_info_types_base.h"

#include <ydb/core/protos/sys_view_types.pb.h>

namespace NKikimr {
namespace NSchemeShard {

struct TSysViewInfo : TSimpleRefCount<TSysViewInfo> {
    using TPtr = TIntrusivePtr<TSysViewInfo>;

    ui64 AlterVersion = 0;
    NKikimrSysView::ESysViewType Type;
};

}
}
