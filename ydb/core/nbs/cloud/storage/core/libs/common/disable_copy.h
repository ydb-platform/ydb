#pragma once

namespace NYdb::NBS {
class TDisableCopyMove
{
public:
    TDisableCopyMove() = default;
    TDisableCopyMove(const TDisableCopyMove&) = delete;
    TDisableCopyMove& operator=(const TDisableCopyMove&) = delete;
    TDisableCopyMove(TDisableCopyMove&&) = delete;
    TDisableCopyMove& operator=(TDisableCopyMove&&) = delete;
};

}   // namespace NYdb::NBS
