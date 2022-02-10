#pragma once

#include "defs.h"
#include "names.h"
#include "logger.h"

#include <ydb/core/tablet_flat/flat_row_state.h>
#include <util/stream/format.h>
#include <util/digest/murmur.h>
#include <util/digest/city.h>
#include <util/digest/fnv.h>

namespace NKikimr {
namespace NTable {
namespace NTest {

    struct TSponge {
        TSponge(ESponge kind) : Kind(kind) { }

        void Describe(IOutputStream &out) const noexcept
        {
            out
                << "Sponge{"
                << NFmt::TLarge(Rows) << "r " << NFmt::TLarge(Cells) << "c"
                << " in " << NFmt::TLarge(Bytes) << "b"
                << " max " << NFmt::TLarge(Upper)
                << ", " << NFmt::Do(Kind) << " " << Hex(Value, HF_FULL)
                << "}";
        }

        void operator()(const TRowState &row) noexcept
        {
            Rows += 1;

            const auto op = row.GetRowState();

            if (Kind == ESponge::None) {

            } else if (op != ERowOp::Upsert && op != ERowOp::Reset) {
                Value += 1;

            } else if (const ui32 cols = row.Size()) {
                for (ui32 it = 0; it < cols; it++) {
                    auto &cell = row.Get(it);

                    (*this)(cell.Data(), cell.Size());
                }
            }
        }

        void operator()(const void *ptr_, ui64 bytes) noexcept
        {
            Bytes += bytes, Cells += 1, Upper = Max(Upper, bytes);

            auto *ptr = static_cast<const char*>(ptr_);

            if (Kind == ESponge::Fnv) {
                Value = FnvHash(ptr, bytes, Value);
            } else if (Kind == ESponge::Murmur) {
                Value = MurmurHash(ptr, bytes, Value);
            } else if (Kind == ESponge::City) {
                Value = CityHash64WithSeed(ptr, bytes, Value);
            } else if (Kind == ESponge::Xor) {
                Value = XorHash(ptr, bytes, Value);
            }
        }

        static ui64 XorHash(const char *ptr, size_t bytes, ui64 val)
        {
            while (bytes) {
                if (bytes >= 8) {
                    val ^= *reinterpret_cast<const ui64*>(ptr);
                    ptr += 8, bytes -= 8;
                } else if (bytes >= 4) {
                    val ^= *reinterpret_cast<const ui32*>(ptr);
                    ptr += 4, bytes -= 4;
                } else if (bytes >= 2) {
                    val ^= *reinterpret_cast<const ui16*>(ptr);
                    ptr += 2, bytes -= 2;
                } else {
                    val ^= *reinterpret_cast<const ui8*>(ptr);
                    ptr += 1, bytes -= 1;
                }
            }

            return val;
        }

        const ESponge Kind = ESponge::None;

    private:
        ui64 Bytes = 0;
        ui64 Rows = 0;
        ui64 Cells = 0;
        ui64 Upper = 0;
        ui64 Value = 0;
    };

}
}
}
