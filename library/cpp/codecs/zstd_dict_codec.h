#pragma once

#include "codecs.h"

#include <util/generic/ptr.h>

namespace NCodecs {
    // benchmarks are here: https://st.yandex-team.ru/SEARCH-1655

    class TZStdDictCodec: public ICodec {
        class TImpl;
        TIntrusivePtr<TImpl> Impl;

    public:
        explicit TZStdDictCodec(ui32 comprLevel = 1);
        ~TZStdDictCodec() override;

        static TStringBuf MyName() {
            return "zstd08d";
        }

        TString GetName() const override;

        ui8 Encode(TStringBuf in, TBuffer& out) const override;

        void Decode(TStringBuf in, TBuffer& out) const override;

        static TVector<TString> ListCompressionNames();
        static int ParseCompressionName(TStringBuf);

    protected:
        void DoLearn(ISequenceReader& in) override;
        bool DoTryToLearn(ISequenceReader& in) final;
        void Save(IOutputStream* out) const override;
        void Load(IInputStream* in) override;
    };

}
