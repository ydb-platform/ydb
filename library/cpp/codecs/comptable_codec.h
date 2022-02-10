#pragma once

#include "codecs.h"

#include <util/generic/ptr.h>

namespace NCodecs {
    class TCompTableCodec: public ICodec {
        class TImpl;
        TIntrusivePtr<TImpl> Impl;

    public:
        enum EQuality {
            Q_LOW = 0,
            Q_HIGH = 1
        };

        explicit TCompTableCodec(EQuality q = Q_HIGH);
        ~TCompTableCodec() override;

        static TStringBuf MyNameHQ() {
            return "comptable-hq";
        }
        static TStringBuf MyNameLQ() {
            return "comptable-lq";
        }

        TString GetName() const override;

        ui8 Encode(TStringBuf in, TBuffer& out) const override;

        void Decode(TStringBuf in, TBuffer& out) const override;

    protected:
        void DoLearn(ISequenceReader& in) override;
        void Save(IOutputStream* out) const override;
        void Load(IInputStream* in) override;
    };

}
