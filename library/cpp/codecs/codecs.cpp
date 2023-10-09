#include "codecs.h"
#include "tls_cache.h"

#include <util/stream/mem.h>

namespace NCodecs {
    void ICodec::Store(IOutputStream* out, TCodecPtr p) {
        if (!p.Get()) {
            ::Save(out, (ui16)0);
            return;
        }

        Y_ENSURE_EX(p->AlreadyTrained(), TCodecException() << "untrained codec " << p->GetName());
        const TString& n = p->GetName();
        Y_ABORT_UNLESS(n.size() <= Max<ui16>());
        ::Save(out, (ui16)n.size());
        out->Write(n.data(), n.size());
        p->Save(out);
    }

    TCodecPtr ICodec::Restore(IInputStream* in) {
        ui16 l = 0;
        ::Load(in, l);

        if (!l) {
            return nullptr;
        }

        TString n;
        n.resize(l);

        Y_ENSURE_EX(in->Load(n.begin(), l) == l, TCodecException());

        TCodecPtr p = ICodec::GetInstance(n);
        p->Load(in);
        p->Trained = true;
        return p;
    }

    TCodecPtr ICodec::RestoreFromString(TStringBuf s) {
        TMemoryInput minp{s.data(), s.size()};
        return Restore(&minp);
    }

    TString ICodec::GetNameSafe(TCodecPtr p) {
        return !p ? TString("none") : p->GetName();
    }

    ui8 TPipelineCodec::Encode(TStringBuf in, TBuffer& out) const {
        size_t res = Traits().ApproximateSizeOnEncode(in.size());
        out.Reserve(res);
        out.Clear();

        if (Pipeline.empty()) {
            out.Append(in.data(), in.size());
            return 0;
        } else if (Pipeline.size() == 1) {
            return Pipeline.front()->Encode(in, out);
        }

        ui8 freelastbits = 0;

        auto buffer = TBufferTlsCache::TlsInstance().Item();
        TBuffer& tmp = buffer.Get();
        tmp.Reserve(res);

        for (auto it = Pipeline.begin(); it != Pipeline.end(); ++it) {
            if (it != Pipeline.begin()) {
                tmp.Clear();
                tmp.Swap(out);
                in = TStringBuf{tmp.data(), tmp.size()};
            }
            freelastbits = (*it)->Encode(in, out);
        }

        return freelastbits;
    }

    void TPipelineCodec::Decode(TStringBuf in, TBuffer& out) const {
        size_t res = Traits().ApproximateSizeOnDecode(in.size());
        out.Reserve(res);
        out.Clear();

        if (Pipeline.empty()) {
            out.Append(in.data(), in.size());
            return;
        } else if (Pipeline.size() == 1) {
            Pipeline.front()->Decode(in, out);
            return;
        }

        auto buffer = TBufferTlsCache::TlsInstance().Item();

        TBuffer& tmp = buffer.Get();
        tmp.Reserve(res);

        for (TPipeline::const_reverse_iterator it = Pipeline.rbegin(); it != Pipeline.rend(); ++it) {
            if (it != Pipeline.rbegin()) {
                tmp.Clear();
                tmp.Swap(out);
                in = TStringBuf{tmp.data(), tmp.size()};
            }
            (*it)->Decode(in, out);
        }
    }

    void TPipelineCodec::Save(IOutputStream* out) const {
        for (const auto& it : Pipeline)
            it->Save(out);
    }

    void TPipelineCodec::Load(IInputStream* in) {
        for (const auto& it : Pipeline) {
            it->Load(in);
            it->SetTrained(true);
        }
    }

    void TPipelineCodec::SetTrained(bool t) {
        for (const auto& it : Pipeline) {
            it->SetTrained(t);
        }
    }

    TPipelineCodec& TPipelineCodec::AddCodec(TCodecPtr codec) {
        if (!codec)
            return *this;

        TCodecTraits tr = codec->Traits();

        if (!MyName) {
            MyTraits.AssumesStructuredInput = tr.AssumesStructuredInput;
            MyTraits.SizeOfInputElement = tr.SizeOfInputElement;
        } else {
            MyName.append(':');
        }

        MyName.append(codec->GetName());
        MyTraits.PreservesPrefixGrouping &= tr.PreservesPrefixGrouping;
        MyTraits.PaddingBit = tr.PaddingBit;
        MyTraits.NeedsTraining |= tr.NeedsTraining;
        MyTraits.Irreversible |= tr.Irreversible;
        MyTraits.SizeOnEncodeAddition = MyTraits.SizeOnEncodeAddition * tr.SizeOnEncodeMultiplier + tr.SizeOnEncodeAddition;
        MyTraits.SizeOnEncodeMultiplier *= tr.SizeOnEncodeMultiplier;
        MyTraits.SizeOnDecodeMultiplier *= tr.SizeOnDecodeMultiplier;
        MyTraits.RecommendedSampleSize = Max(MyTraits.RecommendedSampleSize, tr.RecommendedSampleSize);

        Pipeline.push_back(codec);
        return *this;
    }

    void TPipelineCodec::DoLearnX(ISequenceReader& in, double sampleSizeMult) {
        if (!Traits().NeedsTraining) {
            return;
        }

        if (Pipeline.size() == 1) {
            Pipeline.back()->Learn(in);
            return;
        }

        TVector<TBuffer> trainingInput;

        TStringBuf r;
        while (in.NextRegion(r)) {
            trainingInput.emplace_back(r.data(), r.size());
        }

        TBuffer buff;
        for (const auto& it : Pipeline) {
            it->LearnX(trainingInput.begin(), trainingInput.end(), sampleSizeMult);

            for (auto& bit : trainingInput) {
                buff.Clear();
                it->Encode(TStringBuf{bit.data(), bit.size()}, buff);
                buff.Swap(bit);
            }
        }
    }

    bool TPipelineCodec::AlreadyTrained() const {
        for (const auto& it : Pipeline) {
            if (!it->AlreadyTrained())
                return false;
        }

        return true;
    }

}
