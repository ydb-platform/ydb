#pragma once

#include "cyson_enums.h"

namespace NYsonPull {
    enum class EStreamType {
        Node = YSON_STREAM_TYPE_NODE,
        ListFragment = YSON_STREAM_TYPE_LIST_FRAGMENT,
        MapFragment = YSON_STREAM_TYPE_MAP_FRAGMENT,
    };
}
