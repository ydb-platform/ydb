#pragma once

#include "codecs.h"

#include <util/generic/ptr.h>
#include <util/string/cast.h>

namespace NCodecs {
    // for types greater than char, pipeline with TFreqCodec.

    class THuffmanCodec: public ICodec {
        class TImpl;
        TIntrusivePtr<TImpl> Impl;

    public:
        THuffmanCodec();
        ~THuffmanCodec() override;

        static TStringBuf MyName() {
            return "huffman";
        }

        TString GetName() const override {
            return ToString(MyName());
        }

        ui8 Encode(TStringBuf in, TBuffer& bbb) const override;

        void Decode(TStringBuf in, TBuffer& bbb) const override;

        void LearnByFreqs(const TArrayRef<std::pair<char, ui64>>& freqs);

    protected:
        void DoLearn(ISequenceReader& in) override;
        void Save(IOutputStream* out) const override;
        void Load(IInputStream* in) override;
    };

}
