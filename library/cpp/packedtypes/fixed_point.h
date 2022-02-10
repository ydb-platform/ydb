#pragma once

namespace NFixedPoint {
    template <ui64 FracMult>
    class TFixedPoint {
        typedef TFixedPoint<FracMult> TSelf;
        ui64 Rep;

    public:
        TFixedPoint()
            : Rep()
        {
        }

        template <typename T>
        explicit TFixedPoint(T t)
            : Rep(ui64(t * FracMult))
        {
        }

        explicit TFixedPoint(ui64 i, ui64 f)
            : Rep(i * FracMult + f % FracMult)
        {
        }

        template <typename T>
        TSelf& operator=(T t) {
            Rep = t * FracMult;
            return *this;
        }

        operator double() const {
            return Int() + double(Frac()) / FracMult;
        }

        operator ui64() const {
            return Int();
        }

        template <typename T>
        TSelf& operator/=(T t) {
            Rep = ui64(Rep / t);
            return *this;
        }

        template <typename T>
        TSelf operator/(T t) const {
            TSelf r = *this;
            return r /= t;
        }

        ui64 Frac() const {
            return Rep % FracMult;
        }

        ui64 Int() const {
            return Rep / FracMult;
        }

        static ui64 Mult() {
            return FracMult;
        }

        static TSelf Make(ui64 rep) {
            TFixedPoint<FracMult> fp;
            fp.Rep = rep;
            return fp;
        }
    };

}
