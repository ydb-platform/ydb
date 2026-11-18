#pragma once

namespace NKikimr::NKqp {

enum class ETieringTableKind {
    Standalone,
    InStore,
};

enum class ETieringReboot {
    NoReboot,
    Reboot,
};

}
