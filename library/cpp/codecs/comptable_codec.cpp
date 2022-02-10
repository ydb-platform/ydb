#include "comptable_codec.h"

#include <library/cpp/comptable/comptable.h>
#include <util/string/cast.h>

namespace NCodecs {
    class TCompTableCodec::TImpl: public TAtomicRefCount<TImpl> {
    public:
        TImpl(EQuality q)
            : Quality(q)
        {
        }

        void Init() {
            Compressor.Reset(new NCompTable::TChunkCompressor{(bool)Quality, Table});
            Decompressor.Reset(new NCompTable::TChunkDecompressor{(bool)Quality, Table});
        }

        ui8 Encode(TStringBuf in, TBuffer& out) const {
            out.Clear();
            if (!in) {
                return 0;
            }

            TVector<char> result;
            Compressor->Compress(in, &result);
            out.Assign(&result[0], result.size());
            return 0;
        }

        void Decode(TStringBuf in, TBuffer& out) const {
            out.Clear();
            if (!in) {
                return;
            }

            TVector<char> result;
            Decompressor->Decompress(in, &result);
            out.Assign(&result[0], result.size());
        }

        void DoLearn(ISequenceReader& in) {
            NCompTable::TDataSampler sampler;
            TStringBuf region;
            while (in.NextRegion(region)) {
                if (!region) {
                    continue;
                }

                sampler.AddStat(region);
            }

            sampler.BuildTable(Table);
            Init();
        }

        void Save(IOutputStream* out) const {
            ::Save(out, Table);
        }

        void Load(IInputStream* in) {
            ::Load(in, Table);
            Init();
        }

        NCompTable::TCompressorTable Table;
        THolder<NCompTable::TChunkCompressor> Compressor;
        THolder<NCompTable::TChunkDecompressor> Decompressor;
        const EQuality Quality;
        static const ui32 SampleSize = Max(NCompTable::TDataSampler::Size * 4, (1 << 22) * 5);
    };

    TCompTableCodec::TCompTableCodec(EQuality q)
        : Impl(new TImpl{q})
    {
        MyTraits.NeedsTraining = true;
        MyTraits.SizeOnEncodeMultiplier = 2;
        MyTraits.SizeOnDecodeMultiplier = 10;
        MyTraits.RecommendedSampleSize = TImpl::SampleSize;
    }

    TCompTableCodec::~TCompTableCodec() = default;

    TString TCompTableCodec::GetName() const {
        return ToString(Impl->Quality ? MyNameHQ() : MyNameLQ());
    }

    ui8 TCompTableCodec::Encode(TStringBuf in, TBuffer& out) const {
        return Impl->Encode(in, out);
    }

    void TCompTableCodec::Decode(TStringBuf in, TBuffer& out) const {
        Impl->Decode(in, out);
    }

    void TCompTableCodec::DoLearn(ISequenceReader& in) {
        Impl->DoLearn(in);
    }

    void TCompTableCodec::Save(IOutputStream* out) const {
        Impl->Save(out);
    }

    void TCompTableCodec::Load(IInputStream* in) {
        Impl->Load(in);
    }

}
