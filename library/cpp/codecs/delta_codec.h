#pragma once

#include "codecs.h"

#include <util/generic/array_ref.h>
#include <util/generic/typetraits.h>
#include <util/generic/bitops.h>
#include <util/string/cast.h>

namespace NCodecs {
    template <typename T = ui64, bool UnsignedDelta = true>
    class TDeltaCodec: public ICodec {
        static_assert(std::is_integral<T>::value, "expect std::is_integral<T>::value");

    public:
        using TUnsigned = std::make_unsigned_t<T>;
        using TSigned = std::make_signed_t<T>;
        using TDelta = std::conditional_t<UnsignedDelta, TUnsigned, TSigned>;

    private:
        const TDelta MinDelta{Min<TDelta>()};
        const TDelta MaxDelta{Max<TDelta>() - 1};
        const TDelta InvalidDelta{MaxDelta + 1};

        Y_FORCE_INLINE static TDelta AddSafe(TUnsigned a, TUnsigned b) {
            return a + b;
        }

        Y_FORCE_INLINE static TDelta SubSafe(TUnsigned a, TUnsigned b) {
            return a - b;
        }

    public:
        struct TDecoder {
            const TDelta InvalidDelta{Max<TDelta>()};

            T Last = 0;
            T Result = 0;

            bool First = true;
            bool Invalid = false;

            Y_FORCE_INLINE bool Decode(TDelta t) {
                if (Y_UNLIKELY(First)) {
                    First = false;
                    Result = Last = t;
                    return true;
                }

                if (Y_UNLIKELY(Invalid)) {
                    Invalid = false;
                    Last = 0;
                    Result = t;
                    return true;
                }

                Result = (Last += t);
                Invalid = t == InvalidDelta;

                return !Invalid;
            }
        };

    public:
        static TStringBuf MyName();

        TDeltaCodec() {
            MyTraits.SizeOfInputElement = sizeof(T);
            MyTraits.AssumesStructuredInput = true;
        }

        TString GetName() const override {
            return ToString(MyName());
        }

        template <class TItem>
        static void AppendTo(TBuffer& b, TItem t) {
            b.Append((char*)&t, sizeof(t));
        }

        ui8 Encode(TStringBuf s, TBuffer& b) const override {
            b.Clear();
            if (s.empty()) {
                return 0;
            }

            b.Reserve(s.size());
            TArrayRef<const T> tin{(const T*)s.data(), s.size() / sizeof(T)};

            const T* it = tin.begin();
            TDelta last = *(it++);
            AppendTo(b, last);

            TDelta maxt = SubSafe(MaxDelta, last);
            TDelta mint = AddSafe(MinDelta, last);

            for (; it != tin.end(); ++it) {
                TDelta t = *it;

                if (Y_LIKELY((t >= mint) & (t <= maxt))) {
                    AppendTo(b, t - last);
                    last = t;
                    maxt = SubSafe(MaxDelta, last);
                    mint = AddSafe(MinDelta, last);
                } else {
                    // delta overflow
                    AppendTo(b, InvalidDelta);
                    AppendTo(b, t);
                    last = 0;
                    maxt = MaxDelta;
                    mint = MinDelta;
                }
            }

            return 0;
        }

        void Decode(TStringBuf s, TBuffer& b) const override {
            b.Clear();
            if (s.empty()) {
                return;
            }

            b.Reserve(s.size());
            TArrayRef<const T> tin{(const T*)s.data(), s.size() / sizeof(T)};

            TDecoder dec;

            for (const T* it = tin.begin(); it != tin.end(); ++it) {
                T tmp;
                memcpy(&tmp, it, sizeof(tmp));
                if (dec.Decode(tmp)) {
                    AppendTo(b, dec.Result);
                }
            }
        }

    protected:
        void DoLearn(ISequenceReader&) override {
        }
    };

}
