#pragma once

#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <util/generic/ptr.h>

namespace NKikimr {
namespace NSchemeShard {

struct TStreamingQueryInfo : TSimpleRefCount<TStreamingQueryInfo> {
    using TPtr = TIntrusivePtr<TStreamingQueryInfo>;

    ui64 AlterVersion = 0;
    NKikimrSchemeOp::TStreamingQueryProperties Properties;
};

}
}
