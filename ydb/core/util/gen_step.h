#pragma once

#include <ydb/core/base/logoblob.h>

#include <util/generic/string.h>
#include <util/stream/output.h>
#include <util/stream/str.h>

namespace NKikimr {

    class TGenStep {
        ui64 Value = 0;

    public:
        TGenStep() = default;
        TGenStep(const TGenStep&) = default;
        TGenStep &operator=(const TGenStep&) = default;

        explicit TGenStep(ui64 value)
            : Value(value)
        {}

        TGenStep(ui32 gen, ui32 step)
            : Value(ui64(gen) << 32 | step)
        {}

        explicit TGenStep(const TLogoBlobID& id)
            : TGenStep(id.Generation(), id.Step())
        {}

        explicit operator ui64() const {
            return Value;
        }

        ui32 Generation() const {
            return Value >> 32;
        }

        ui32 Step() const {
            return Value;
        }

        void Output(IOutputStream& s) const {
            s << Generation() << ":" << Step();
        }

        TString ToString() const {
            TStringStream s;
            Output(s);
            return s.Str();
        }

        TGenStep Previous() const {
            Y_ABORT_UNLESS(Value);
            return TGenStep(Value - 1);
        }

        friend auto operator <=>(const TGenStep& x, const TGenStep& y) = default;
    };

} // NKikimr
