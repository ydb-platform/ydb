#pragma once

#include "codecs.h"

#include "delta_codec.h"
#include "tls_cache.h"

#include <library/cpp/bit_io/bitinput.h>
#include <library/cpp/bit_io/bitoutput.h>
#include <util/string/cast.h>

namespace NCodecs {
    template <typename T, bool WithDelta = false>
    class TPForCodec: public ICodec {
        using TUnsigned = std::make_unsigned_t<T>;
        typedef TDeltaCodec<TUnsigned> TDCodec;

        typedef std::conditional_t<WithDelta, typename TDCodec::TDelta, T> TValue;
        static_assert(std::is_unsigned<TValue>::value, "expect std:is_unsigned<TValue>::value");

        static const ui64 BitsInT = sizeof(TUnsigned) * 8;

        TDCodec DeltaCodec;

    public:
        static TStringBuf MyName();

        TPForCodec() {
            MyTraits.AssumesStructuredInput = true;
            MyTraits.SizeOfInputElement = sizeof(T);
            MyTraits.SizeOnDecodeMultiplier = sizeof(T);
        }

        TString GetName() const override {
            return ToString(MyName());
        }

        ui8 Encode(TStringBuf s, TBuffer& b) const override {
            b.Clear();
            if (s.empty()) {
                return 0;
            }

            b.Reserve(2 * s.size() + b.Size());

            if (WithDelta) {
                auto buffer = TBufferTlsCache::TlsInstance().Item();
                TBuffer& db = buffer.Get();
                db.Clear();
                db.Reserve(2 * s.size());
                DeltaCodec.Encode(s, db);
                s = TStringBuf{db.data(), db.size()};
            }

            TArrayRef<const TValue> tin{(const TValue*)s.data(), s.size() / sizeof(TValue)};

            const ui64 sz = tin.size();
            ui64 bitcounts[BitsInT + 1];
            Zero(bitcounts);

            ui32 zeros = 0;

            for (const TValue* it = tin.begin(); it != tin.end(); ++it) {
                TUnsigned v = 1 + (TUnsigned)*it;
                ui64 l = MostSignificantBit(v) + 1;
                ++bitcounts[l];

                if (!v) {
                    ++zeros;
                }
            }

            // cumulative bit counts
            for (ui64 i = 0; i < BitsInT; ++i) {
                bitcounts[i + 1] += bitcounts[i];
            }

            bool hasexceptions = zeros;
            ui64 optimalbits = BitsInT;

            {
                ui64 excsize = 0;
                ui64 minsize = sz * BitsInT;

                for (ui64 current = BitsInT; current; --current) {
                    ui64 size = bitcounts[current] * current + (sz - bitcounts[current]) * (current + 6 + excsize) + zeros * (current + 6);

                    excsize += current * bitcounts[current];

                    if (size < minsize) {
                        minsize = size;
                        optimalbits = current;
                        hasexceptions = zeros || sz - bitcounts[current];
                    }
                }
            }

            if (!optimalbits || BitsInT == optimalbits) {
                b.Append((ui8)-1);
                b.Append(s.data(), s.size());
                return 0;
            } else {
                NBitIO::TBitOutputVector<TBuffer> bout(&b);
                bout.Write(0, 1);
                bout.Write(hasexceptions, 1);
                bout.Write(optimalbits, 6);

                for (const TValue* it = tin.begin(); it != tin.end(); ++it) {
                    TUnsigned word = 1 + (TUnsigned)*it;
                    ui64 len = MostSignificantBit(word) + 1;
                    if (len > optimalbits || !word) {
                        Y_ENSURE(hasexceptions, " ");
                        bout.Write(0, optimalbits);
                        bout.Write(len, 6);
                        bout.Write(word, len);
                    } else {
                        bout.Write(word, optimalbits);
                    }
                }

                return bout.GetByteReminder();
            } // the rest of the last byte is zero padded. BitsInT is always > 7.
        }

        void Decode(TStringBuf s, TBuffer& b) const override {
            b.Clear();
            if (s.empty()) {
                return;
            }

            b.Reserve(s.size() * sizeof(T) + b.Size());

            ui64 isplain = 0;
            ui64 hasexceptions = 0;
            ui64 bits = 0;

            NBitIO::TBitInput bin(s);
            bin.ReadK<1>(isplain);
            bin.ReadK<1>(hasexceptions);
            bin.ReadK<6>(bits);

            if (Y_UNLIKELY(isplain)) {
                s.Skip(1);

                if (WithDelta) {
                    DeltaCodec.Decode(s, b);
                } else {
                    b.Append(s.data(), s.size());
                }
            } else {
                typename TDCodec::TDecoder decoder;

                if (hasexceptions) {
                    ui64 word = 0;
                    while (bin.Read(word, bits)) {
                        if (word || (bin.ReadK<6>(word) && bin.Read(word, word))) {
                            --word;

                            TValue t = word;

                            if (WithDelta) {
                                if (decoder.Decode(t)) {
                                    TStringBuf r{(char*)&decoder.Result, sizeof(decoder.Result)};
                                    b.Append(r.data(), r.size());
                                }
                            } else {
                                TStringBuf r{(char*)&t, sizeof(t)};
                                b.Append(r.data(), r.size());
                            }
                        }
                    }
                } else {
                    ui64 word = 0;
                    T outarr[256 / sizeof(T)];
                    ui32 cnt = 0;
                    while (true) {
                        ui64 v = bin.Read(word, bits);

                        if ((!v) | (!word))
                            break;

                        --word;
                        TValue t = word;

                        if (WithDelta) {
                            if (decoder.Decode(t)) {
                                outarr[cnt++] = decoder.Result;
                            }
                        } else {
                            outarr[cnt++] = t;
                        }

                        if (cnt == Y_ARRAY_SIZE(outarr)) {
                            b.Append((const char*)outarr, sizeof(outarr));
                            cnt = 0;
                        }
                    }

                    if (cnt) {
                        b.Append((const char*)outarr, cnt * sizeof(T));
                    }
                }
            }
        }

    protected:
        void DoLearn(ISequenceReader&) override {
        }
    };

}
