#include "solar_codec.h"

#include <library/cpp/codecs/greedy_dict/gd_builder.h>

#include <library/cpp/containers/comptrie/comptrie_builder.h>
#include <library/cpp/string_utils/relaxed_escaper/relaxed_escaper.h>
#include <util/stream/length.h>
#include <util/string/printf.h>
#include <util/ysaveload.h>

namespace NCodecs {
    static inline ui32 Append(TBuffer& pool, TStringBuf data) {
        pool.Append(data.data(), data.size());
        return pool.Size();
    }

    void TSolarCodec::DoLearn(ISequenceReader& r) {
        using namespace NGreedyDict;

        const ui32 maxlen = Max<ui32>() / Max<ui32>(MaxEntries, 1);

        Decoder.clear();
        Pool.Clear();

        THolder<TEntrySet> set;

        {
            TMemoryPool pool(8112, TMemoryPool::TLinearGrow::Instance());
            TStringBufs bufs;

            TStringBuf m;
            while (r.NextRegion(m)) {
                bufs.push_back(pool.AppendString(m));
            }

            {
                TDictBuilder b(Settings);
                b.SetInput(bufs);
                b.Build(MaxEntries, MaxIterations, maxlen);

                set = b.ReleaseEntrySet();
            }
        }

        set->SetScores(ES_LEN_COUNT);

        {
            TVector<std::pair<float, TStringBuf>> tmp;
            tmp.reserve(set->size());

            for (const auto& it : *set) {
                Y_ENSURE(it.Str.Size() <= maxlen);
                tmp.push_back(std::make_pair(-it.Score, it.Str));
            }

            Sort(tmp.begin(), tmp.end());

            Decoder.reserve(tmp.size() + 1);
            Decoder.push_back(0);

            for (const auto& it : tmp) {
                Y_ENSURE(Decoder.back() == Pool.Size(), "learning invariant failed");
                ui32 endoff = Append(Pool, it.second);
                Decoder.push_back(endoff);
            }
        }

        Pool.ShrinkToFit();
        Decoder.shrink_to_fit();

        TBufferOutput bout;

        {
            TVector<std::pair<TStringBuf, ui32>> tmp2;
            tmp2.reserve(Decoder.size());

            for (ui32 i = 1, sz = Decoder.size(); i < sz; ++i) {
                TStringBuf s = DoDecode(i);
                tmp2.push_back(std::make_pair(s, i - 1));
                Y_ENSURE(s.size() == (Decoder[i] - Decoder[i - 1]), "learning invariant failed");
            }

            Sort(tmp2.begin(), tmp2.end());

            {
                TEncoder::TBuilder builder(CTBF_PREFIX_GROUPED);
                for (const auto& it : tmp2) {
                    builder.Add(it.first.data(), it.first.size(), it.second);
                }

                builder.Save(bout);
            }
        }

        Encoder.Init(TBlob::FromBuffer(bout.Buffer()));
    }

    void TSolarCodec::Save(IOutputStream* out) const {
        TBlob b = Encoder.Data();
        ::Save(out, (ui32)b.Size());
        out->Write(b.Data(), b.Size());
    }

    void TSolarCodec::Load(IInputStream* in) {
        ui32 sz;
        ::Load(in, sz);
        TLengthLimitedInput lin(in, sz);
        Encoder.Init(TBlob::FromStream(lin));
        Pool.Clear();
        Decoder.clear();

        TVector<std::pair<ui32, TString>> tmp;

        ui32 poolsz = 0;
        for (TEncoder::TConstIterator it = Encoder.Begin(); it != Encoder.End(); ++it) {
            const TString& s = it.GetKey();
            tmp.push_back(std::make_pair(it.GetValue(), !s ? TString("\0", 1) : s));
            poolsz += Max<ui32>(s.size(), 1);
        }

        Sort(tmp.begin(), tmp.end());

        Pool.Reserve(poolsz);
        Decoder.reserve(tmp.size() + 1);
        Decoder.push_back(0);

        for (ui32 i = 0, sz2 = tmp.size(); i < sz2; ++i) {
            Y_ENSURE(i == tmp[i].first, "oops! " << i << " " << tmp[i].first);
            Decoder.push_back(Append(Pool, tmp[i].second));
        }

        Pool.ShrinkToFit();
        Decoder.shrink_to_fit();
    }

}
