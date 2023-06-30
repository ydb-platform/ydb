#pragma once

#include "ypath_client.h"

#include <yt/yt_proto/yt/core/ytree/proto/ypath.pb.h>

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

struct TYPathProxy
{
    DEFINE_YPATH_PROXY(Node);

    DEFINE_YPATH_PROXY_METHOD(NProto, GetKey);
    DEFINE_YPATH_PROXY_METHOD(NProto, Get);
    DEFINE_YPATH_PROXY_METHOD(NProto, List);
    DEFINE_YPATH_PROXY_METHOD(NProto, Exists);
    DEFINE_MUTATING_YPATH_PROXY_METHOD(NProto, Set);
    DEFINE_MUTATING_YPATH_PROXY_METHOD(NProto, MultisetAttributes);
    DEFINE_MUTATING_YPATH_PROXY_METHOD(NProto, Remove);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
