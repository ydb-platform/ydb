#pragma once

#include "codecs.h"
#include <library/cpp/containers/comptrie/comptrie_trie.h>
#include <library/cpp/codecs/greedy_dict/gd_builder.h>

#include <util/string/cast.h>
#include <util/string/escape.h>

namespace NCodecs {
    // TODO: Попробовать добавлять в словарь вместе с намайненными словами также их суффиксы.
    // TODO: Возможно удастся, не слишком потеряв в сжатии, выиграть в робастности к небольшим изменениям в корпусе.

    struct TVarIntTraits {
        static const size_t MAX_VARINT32_BYTES = 5;

        static void Write(ui32 value, TBuffer& b) {
            while (value > 0x7F) {
                b.Append(static_cast<ui8>(value) | 0x80);
                value >>= 7;
            }
            b.Append(static_cast<ui8>(value) & 0x7F);
        }

        static void Read(TStringBuf& r, ui32& value) {
            ui32 result = 0;
            for (ui32 count = 0; count < MAX_VARINT32_BYTES; ++count) {
                const ui32 b = static_cast<ui8>(r[0]);
                r.Skip(1);
                result |= static_cast<ui32>(b & 0x7F) << (7 * count);
                if (!(b & 0x80)) {
                    value = result;
                    return;
                } else if (Y_UNLIKELY(r.empty())) {
                    break;
                }
            }
            Y_ENSURE_EX(false, TCodecException() << "Bad data");
        }
    };

    struct TShortIntTraits {
        static const size_t SHORTINT_SIZE_LIMIT = 0x8000;

        Y_FORCE_INLINE static void Write(ui32 value, TBuffer& b) {
            Y_ENSURE_EX(value < SHORTINT_SIZE_LIMIT, TCodecException() << "Bad write method");
            if (value >= 0x80) {
                b.Append(static_cast<ui8>(value >> 8) | 0x80);
            }
            b.Append(static_cast<ui8>(value));
        }

        Y_FORCE_INLINE static void Read(TStringBuf& r, ui32& value) {
            ui32 result = static_cast<ui8>(r[0]);
            r.Skip(1);
            if (result >= 0x80) {
                Y_ENSURE_EX(!r.empty(), TCodecException() << "Bad data");
                result = ((result << 8) & 0x7FFF) | static_cast<ui8>(r[0]);
                r.Skip(1);
            }
            value = result;
        }
    };

    class TSolarCodec: public ICodec {
    public:
        static TStringBuf MyName8k() {
            return TStringBuf("solar-8k");
        }
        static TStringBuf MyName16k() {
            return TStringBuf("solar-16k");
        }
        static TStringBuf MyName32k() {
            return TStringBuf("solar-32k");
        }
        static TStringBuf MyName64k() {
            return TStringBuf("solar-64k");
        }
        static TStringBuf MyName256k() {
            return TStringBuf("solar-256k");
        }
        static TStringBuf MyName() {
            return TStringBuf("solar");
        }
        static TStringBuf MyName8kAdapt() {
            return TStringBuf("solar-8k-a");
        }
        static TStringBuf MyName16kAdapt() {
            return TStringBuf("solar-16k-a");
        }
        static TStringBuf MyName32kAdapt() {
            return TStringBuf("solar-32k-a");
        }
        static TStringBuf MyName64kAdapt() {
            return TStringBuf("solar-64k-a");
        }
        static TStringBuf MyName256kAdapt() {
            return TStringBuf("solar-256k-a");
        }
        static TStringBuf MyNameShortInt() {
            return TStringBuf("solar-si");
        }

        explicit TSolarCodec(ui32 maxentries = 1 << 14, ui32 maxiter = 16, const NGreedyDict::TBuildSettings& s = NGreedyDict::TBuildSettings())
            : Settings(s)
            , MaxEntries(maxentries)
            , MaxIterations(maxiter)
        {
            MyTraits.NeedsTraining = true;
            MyTraits.SizeOnDecodeMultiplier = 2;
            MyTraits.RecommendedSampleSize = maxentries * s.GrowLimit * maxiter * 8;
        }

        ui8 /*free bits in last byte*/ Encode(TStringBuf r, TBuffer& b) const override {
            EncodeImpl<TVarIntTraits>(r, b);
            return 0;
        }

        void Decode(TStringBuf r, TBuffer& b) const override {
            DecodeImpl<TVarIntTraits>(r, b);
        }

        TString GetName() const override {
            return ToString(MyName());
        }

    protected:
        void DoLearn(ISequenceReader&) override;
        void Save(IOutputStream*) const override;
        void Load(IInputStream*) override;

        Y_FORCE_INLINE TStringBuf SubStr(ui32 begoff, ui32 endoff) const {
            return TStringBuf(Pool.Data() + begoff, endoff - begoff);
        }

        Y_FORCE_INLINE TStringBuf DoDecode(ui32 num) const {
            return SubStr(Decoder[num - 1], Decoder[num]);
        }

        template <class TTraits>
        Y_FORCE_INLINE void EncodeImpl(TStringBuf r, TBuffer& b) const {
            b.Clear();
            b.Reserve(r.size());
            while (!r.empty()) {
                size_t sz = 0;
                ui32 val = (ui32)-1;
                Encoder.FindLongestPrefix(r, &sz, &val);
                TTraits::Write(val + 1, b);
                r.Skip(Max<size_t>(sz, 1));
            }
        }

        template <class TTraits>
        Y_FORCE_INLINE void DecodeImpl(TStringBuf r, TBuffer& b) const {
            b.Clear();
            b.Reserve(r.size());
            ui32 v = 0;
            while (!r.empty()) {
                TTraits::Read(r, v);
                TStringBuf s = DoDecode(v);
                b.Append(s.data(), s.size());
            }
        }

        inline bool CanUseShortInt() const {
            return Decoder.size() < TShortIntTraits::SHORTINT_SIZE_LIMIT;
        }

    private:
        typedef TCompactTrie<char, ui32> TEncoder;
        typedef TVector<ui32> TDecoder;

        TBuffer Pool;
        TEncoder Encoder;
        TDecoder Decoder;

        NGreedyDict::TBuildSettings Settings;
        ui32 MaxEntries;
        ui32 MaxIterations;
    };

    // Uses varints or shortints depending on the decoder size
    class TAdaptiveSolarCodec: public TSolarCodec {
    public:
        explicit TAdaptiveSolarCodec(ui32 maxentries = 1 << 14, ui32 maxiter = 16, const NGreedyDict::TBuildSettings& s = NGreedyDict::TBuildSettings())
            : TSolarCodec(maxentries, maxiter, s)
        {
        }

        ui8 /*free bits in last byte*/ Encode(TStringBuf r, TBuffer& b) const override {
            if (CanUseShortInt()) {
                EncodeImpl<TShortIntTraits>(r, b);
            } else {
                EncodeImpl<TVarIntTraits>(r, b);
            }

            return 0;
        }

        void Decode(TStringBuf r, TBuffer& b) const override {
            if (CanUseShortInt()) {
                DecodeImpl<TShortIntTraits>(r, b);
            } else {
                DecodeImpl<TVarIntTraits>(r, b);
            }
        }

        TString GetName() const override {
            if (CanUseShortInt()) {
                return ToString(MyNameShortInt());
            } else {
                return ToString(MyName());
            }
        }
    };

    class TSolarCodecShortInt: public TSolarCodec {
    public:
        explicit TSolarCodecShortInt(ui32 maxentries = 1 << 14, ui32 maxiter = 16, const NGreedyDict::TBuildSettings& s = NGreedyDict::TBuildSettings())
            : TSolarCodec(maxentries, maxiter, s)
        {
        }

        ui8 /*free bits in last byte*/ Encode(TStringBuf r, TBuffer& b) const override {
            EncodeImpl<TShortIntTraits>(r, b);
            return 0;
        }

        void Decode(TStringBuf r, TBuffer& b) const override {
            DecodeImpl<TShortIntTraits>(r, b);
        }

        TString GetName() const override {
            return ToString(MyNameShortInt());
        }

    protected:
        void Load(IInputStream* in) override {
            TSolarCodec::Load(in);
            Y_ENSURE_EX(CanUseShortInt(), TCodecException() << "Bad data");
        }
    };

}
