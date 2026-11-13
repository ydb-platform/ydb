#pragma once

namespace NKikimr {

enum class EMoveDataYieldMutation {
    None,
    DeleteCursor,
    DeleteLastCursor,
    DeleteAll,
};

} // namespace NKikimr
