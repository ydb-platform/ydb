#pragma once

#include <library/cpp/codecs/codecs.h>
#include <library/cpp/packedtypes/longs.h>

namespace NCodecs {

    ////////////////////////////////////////////////////////////////////////////
    // TVarLengthIntCodec
    ////////////////////////////////////////////////////////////////////////////
    template <class TNumber>
    class TVarLengthIntCodec : public ICodec {
    public:
        TVarLengthIntCodec(double compressionRatio = 2.0)
            : CompressionRatio(compressionRatio)
        {}

        TString GetName() const override { return "VarLengthInt"; }

        ui8 Encode(TStringBuf in, TBuffer& out) const override {
            if (in.size() / sizeof(TNumber) * sizeof(TNumber) != in.size())
                throw TCodecException();

            const TNumber *pos = (const TNumber *)(in.begin());
            const TNumber *end = (const TNumber *)(in.end());

            out.Reserve(in.size() / CompressionRatio);

            char buf[16];
            while (pos != end) {
                const i64 v = (i64)(*pos);
                int s = out_long(v, buf);
                out.Append(buf, s);
                ++pos;
            }
            return 0;
        }

        void Decode(TStringBuf in, TBuffer& out) const override {
            const char *pos = in.begin();
            const char *end = in.end();

            out.Reserve(in.size() * CompressionRatio);

            while (pos < end) {
                i64 v;
                pos += in_long(v, pos);
                TNumber n = (TNumber)v;
                out.Append((const char *)&n, sizeof(n));
            }

            Y_ASSERT(pos == end);
        }

    protected:
        void DoLearn(ISequenceReader&) override {}

        double CompressionRatio;
    };


    ////////////////////////////////////////////////////////////////////////////
    // TRunLengthCodec
    ////////////////////////////////////////////////////////////////////////////
    template <class TInputNumber, class TOutputNumber = TInputNumber>
    class TRunLengthCodec : public ICodec {
    public:
        TRunLengthCodec(double compressionRatio = 4)
            : CompressionRatio(compressionRatio)
        {}

        TString GetName() const override { return "RunLength"; }

        ui8 Encode(TStringBuf in, TBuffer& out) const override {
            if (in.size() / sizeof(TInputNumber) * sizeof(TInputNumber) != in.size())
                throw TCodecException();

            const TInputNumber *pos = (const TInputNumber *)(in.begin());
            const TInputNumber *end = (const TInputNumber *)(in.end());

            out.Reserve(in.size() / sizeof(TInputNumber) * sizeof(TOutputNumber) / CompressionRatio);

            while (pos != end) {
                TOutputNumber val = *pos;
                TOutputNumber rep = 1;
                ++pos;
                while (pos != end && *pos == val) {
                    ++rep;
                    ++pos;
                }
                out.Append((const char *)&val, sizeof(TOutputNumber));
                out.Append((const char *)&rep, sizeof(TOutputNumber));
            }
            return 0;
        }

        void Decode(TStringBuf in, TBuffer& out) const override {
            if (in.size() / sizeof(TOutputNumber) * sizeof(TOutputNumber) != in.size())
                throw TCodecException();

            if (in.size() / sizeof(TOutputNumber) % 2 != 0)
                throw TCodecException();

            const TOutputNumber *pos = (const TOutputNumber *)(in.begin());
            const TOutputNumber *end = (const TOutputNumber *)(in.end());

            out.Reserve(in.size() / sizeof(TOutputNumber) * sizeof(TInputNumber) * CompressionRatio);

            while (pos != end) {
                TInputNumber val = (TInputNumber)*pos;
                ++pos;
                TOutputNumber rep = *pos;
                ++pos;
                for (TOutputNumber i = 0; i < rep; ++i)
                    out.Append((const char *)&val, sizeof(TInputNumber));
            }
        }

    protected:
        void DoLearn(ISequenceReader&) override {}

        double CompressionRatio;
    };

    ////////////////////////////////////////////////////////////////////////////
    // TSemiSortedDeltaCodec
    ////////////////////////////////////////////////////////////////////////////
    template <class TInputNumber, class TOutputNumber = TInputNumber>
    class TSemiSortedDeltaCodec : public ICodec {
    public:
        TSemiSortedDeltaCodec(double compressionRatio = 0.8)
            : CompressionRatio(compressionRatio)
        {}

        TString GetName() const override { return "SemiSortedDelta"; }

        ui8 Encode(TStringBuf in, TBuffer& out) const override {
            if (in.size() / sizeof(TInputNumber) * sizeof(TInputNumber) != in.size())
                throw TCodecException();

            const TInputNumber *pos = (const TInputNumber *)(in.begin());
            const TInputNumber *end = (const TInputNumber *)(in.end());

            out.Reserve(in.size() / sizeof(TInputNumber) * sizeof(TOutputNumber) / CompressionRatio);

            while (pos != end) {
                size_t nOffs = out.Size();
                TOutputNumber size = 1;
                out.Append((const char *)&size, sizeof(TOutputNumber));
                TOutputNumber val = *pos;
                ++pos;
                out.Append((const char *)&val, sizeof(TOutputNumber));
                while (pos != end && *pos >= val) {
                    ++size;
                    TOutputNumber diff = *pos - val;
                    val = *pos;
                    ++pos;
                    out.Append((const char *)&diff, sizeof(TOutputNumber));
                }
                if (size > 1) {
                    *(TOutputNumber *)(out.Data() + nOffs) = size;
                }
            }
            return 0;
        }

        void Decode(TStringBuf in, TBuffer& out) const override {
            if (in.size() / sizeof(TOutputNumber) * sizeof(TOutputNumber) != in.size())
                throw TCodecException();

            const TOutputNumber *pos = (const TOutputNumber *)(in.begin());
            const TOutputNumber *end = (const TOutputNumber *)(in.end());

            out.Reserve(in.size() / sizeof(TOutputNumber) * sizeof(TInputNumber) * CompressionRatio);

            while (pos != end) {
                TOutputNumber size = *pos;
                ++pos;
                TOutputNumber val = 0;
                for (TOutputNumber i = 0; i < size; ++i, ++pos) {
                    val += *pos;
                    TInputNumber v = val;
                    out.Append((const char *)&v, sizeof(TInputNumber));
                }
            }
        }

    protected:
        void DoLearn(ISequenceReader&) override {}
        double CompressionRatio;
    };


} // NCodecs
