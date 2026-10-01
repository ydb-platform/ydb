#pragma once

namespace NKikimr::NKqp {

enum class ETieringObjectKeyTree {
    Disabled,
    Enabled,
};

enum class ETieringTableLocation {
    Standalone,
    InStore,
};

}
