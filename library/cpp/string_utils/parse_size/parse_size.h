#pragma once

#include <util/generic/strbuf.h>

namespace NSize {
    ui64 ParseSize(TStringBuf size);

    // Convenient disk size representation with string parsing and integer comparison
    class TSize {
    public:
        TSize(ui64 value = 0)
            : Value(value)
        {
        }

        ui64 GetValue() const {
            return Value;
        }

        operator ui64() const {
            return Value;
        }

    private:
        ui64 Value;
    };

    TSize FromKiloBytes(ui64 value);
    TSize FromMegaBytes(ui64 value);
    TSize FromGigaBytes(ui64 value);
    TSize FromTeraBytes(ui64 value);

}
