#pragma once

#include "schemeshard_info_types_base.h"

namespace NKikimr {
namespace NSchemeShard {

struct TStreamingQueryInfo : TSimpleRefCount<TStreamingQueryInfo> {
    using TPtr = TIntrusivePtr<TStreamingQueryInfo>;

    ui64 AlterVersion = 0;
    NKikimrSchemeOp::TStreamingQueryProperties Properties;
};

}
}
