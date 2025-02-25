#pragma once

#include <optional>
#include <vector>

#include "retro_span.h"
#include "span_circlebuf_stats.h"

#include <util/system/spinlock.h>

namespace NRetro {

struct TSpanCircleBufStats;

class TSpanCircleBuf {
public:
    TSpanCircleBuf(ui32 cellsNum = 100000);

    template <typename T> requires std::derived_from<T, TRetroSpan>
    void Write(const T& span) {
        Write(span.GetType(), reinterpret_cast<const ui8*>(&span));
    }

    void Write(ERetroSpanType type, const ui8* span);
    std::vector<TRetroSpan::TPtr> ReadSpansOfTrace(ui64 traceId);

    std::shared_ptr<TSpanCircleBufStats> GetStatsSnapshot();

private:
    constexpr static ui32 CellSize = 1024;

    struct TCell {
        TCell() : Type(ERetroSpanType::None), Data{0} {}

        TRetroSpan* GetSpanPtr() {
            return reinterpret_cast<TRetroSpan*>(&Data);
        }

        ERetroSpanType Type;
        ui8 Data[CellSize - sizeof(Type)];
    };

private:
    void CreateStatsSnapshot();

private:
    TSpinLock ReadLock;
    std::vector<TCell> Buffer;
    ui32 Head;

    std::shared_ptr<TSpanCircleBufStats> StatsSnapshot;
    TSpanCircleBufStats Stats;
};

} // namespace NRetro
