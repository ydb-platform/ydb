#pragma once

namespace NKikimr::NOlap::NActualizer {

// Which actualizers an extraction pass draws from: the move runs on its own driver, tiering on the tablet's loop.
enum class EActualizationScope {
    All,
    MoveDataOnly,
    ExceptMoveData,
};

}   // namespace NKikimr::NOlap::NActualizer
