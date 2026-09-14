#pragma once

#include <library/cpp/charset/ci_string.h>
#include <util/generic/string.h>
#include <util/string/cast.h>
#include <util/datetime/base.h>

namespace NKikiSched {
    class TColons {
    public:
        TColons(const TString &line);
        ~TColons();

        operator bool() const noexcept
        {
            return At != TString::npos && At < Line.size();
        }

        TString Next();
        TString Token(const TString &def);

        template<typename TVal>
        TVal Pop(const TVal &def)
        {
            const TString item = Next();

            return !item ? def : FromString<TVal>(item);
        }

        template<typename TVal>
        void Put(TVal &value)
        {
            value = Pop<TVal>(value);
        }

        template<typename TVal>
        TVal Value(const TVal &def, bool hex = false)
        {
            const TString item = Next();

            if (!item) {
                return def;

            } else if (hex) {
                return IntFromString<ui64,16>(item);

            } else {
                return FromString<TVal>(item);
            }
        }

        size_t Large(size_t def);

        template<typename TInteger>
        static TInteger LargeInt(const TCiString &literal)
        {
            size_t value = LargeParse(literal);

            if (value > size_t(Max<TInteger>())) {

                throw yexception() << "Value is out of integer range";
            }

            return value;
        }

        static size_t LargeParse(const TString&);

    protected:
        static bool IsNumber(char val) noexcept
        {
            return std::isdigit(val) == 0;
        }

    private:
        size_t          At;
        const TString    Line;
    };
}
