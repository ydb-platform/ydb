#pragma once

#include "sample.h"

#include <util/generic/bt_exception.h>
#include <util/generic/hash.h>
#include <util/generic/ptr.h>
#include <util/generic/singleton.h>

#include <util/stream/input.h>
#include <util/stream/output.h>

#include <util/string/cast.h>
#include <util/string/vector.h>
#include <util/system/tls.h>
#include <util/ysaveload.h>

namespace NCodecs {
    class TCodecException: public TWithBackTrace<yexception> {};

    class ICodec;

    using TCodecPtr = TIntrusivePtr<ICodec>;
    using TCodecConstPtr = TIntrusiveConstPtr<ICodec>;

    struct TCodecTraits {
        ui32 RecommendedSampleSize = 0;
        ui16 SizeOfInputElement = 1;
        ui8 SizeOnEncodeMultiplier = 1;
        ui8 SizeOnEncodeAddition = 0;
        ui8 SizeOnDecodeMultiplier = 1;

        bool NeedsTraining = false;
        bool PreservesPrefixGrouping = false;
        bool Irreversible = false;
        bool PaddingBit = 0;
        bool AssumesStructuredInput = false;

        size_t ApproximateSizeOnEncode(size_t sz) const {
            return sz * SizeOnEncodeMultiplier + SizeOnEncodeAddition;
        }

        size_t ApproximateSizeOnDecode(size_t sz) const {
            return sz * SizeOnDecodeMultiplier;
        }
    };

    class ICodec: public TAtomicRefCount<ICodec> {
    protected:
        bool Trained = false;
        TCodecTraits MyTraits;

    public:
        TCodecTraits Traits() const {
            return MyTraits;
        }

        // the name of the codec (or its variant) to be used in the codec registry
        virtual TString GetName() const = 0;

        virtual ui8 /*free bits in last byte*/ Encode(TStringBuf, TBuffer&) const = 0;
        virtual ui8 Encode(const TBuffer& input, TBuffer& output) const {
            return Encode(TStringBuf(input.Data(), input.Data() + input.Size()), output);
        }
        virtual void Decode(TStringBuf, TBuffer&) const = 0;
        virtual void Decode(const TBuffer& input, TBuffer& output) const {
            Decode(TStringBuf(input.Data(), input.Data() + input.Size()), output);
        }

        virtual ~ICodec() = default;

        virtual bool AlreadyTrained() const {
            return !Traits().NeedsTraining || Trained;
        }
        virtual void SetTrained(bool t) {
            Trained = t;
        }

        bool TryToLearn(ISequenceReader& r) {
            Trained = DoTryToLearn(r);
            return Trained;
        }

        void Learn(ISequenceReader& r) {
            LearnX(r, 1);
        }

        template <class TIter>
        void Learn(TIter beg, TIter end) {
            Learn(beg, end, IterToStringBuf<TIter>);
        }

        template <class TIter, class TGetter>
        void Learn(TIter beg, TIter end, TGetter getter) {
            auto sample = GetSample(beg, end, Traits().RecommendedSampleSize, getter);
            TSimpleSequenceReader<TBuffer> reader{sample};
            Learn(reader);
        }

        static TCodecPtr GetInstance(TStringBuf name);

        static TVector<TString> GetCodecsList();

        static TString GetNameSafe(TCodecPtr p);

        static void Store(IOutputStream* out, TCodecPtr p);
        static TCodecPtr Restore(IInputStream* in);
        static TCodecPtr RestoreFromString(TStringBuf);

    protected:
        virtual void DoLearn(ISequenceReader&) = 0;

        virtual bool DoTryToLearn(ISequenceReader& r) {
            DoLearn(r);
            return true;
        }

        // so the pipeline codec will know to adjust the sample for the subcodecs
        virtual void DoLearnX(ISequenceReader& r, double /*sampleSizeMultiplier*/) {
            DoLearn(r);
        }

        virtual void Save(IOutputStream*) const {
        }
        virtual void Load(IInputStream*) {
        }
        friend class TPipelineCodec;

    public:
        // so the pipeline codec will know to adjust the sample for the subcodecs
        void LearnX(ISequenceReader& r, double sampleSizeMult) {
            DoLearnX(r, sampleSizeMult);
            Trained = true;
        }

        template <class TIter>
        void LearnX(TIter beg, TIter end, double sampleSizeMult) {
            auto sample = GetSample(beg, end, Traits().RecommendedSampleSize * sampleSizeMult);
            TSimpleSequenceReader<TBuffer> reader{sample};
            LearnX(reader, sampleSizeMult);
        }
    };

    class TBasicTrivialCodec: public ICodec {
    public:
        ui8 Encode(TStringBuf in, TBuffer& out) const override {
            out.Assign(in.data(), in.size());
            return 0;
        }

        void Decode(TStringBuf in, TBuffer& out) const override {
            Encode(in, out);
        }

    protected:
        void DoLearn(ISequenceReader&) override {
        }
    };

    class TTrivialCodec: public TBasicTrivialCodec {
    public:
        TTrivialCodec() {
            MyTraits.PreservesPrefixGrouping = true;
        }

        static TStringBuf MyName() {
            return "trivial";
        }

        TString GetName() const override {
            return ToString(MyName());
        }
    };

    class TTrivialTrainableCodec: public TBasicTrivialCodec {
    public:
        TTrivialTrainableCodec() {
            MyTraits.PreservesPrefixGrouping = true;
            MyTraits.NeedsTraining = true;
        }

        static TStringBuf MyName() {
            return "trivial-trainable";
        }

        TString GetName() const override {
            return ToString(MyName());
        }
    };

    class TNullCodec: public ICodec {
    public:
        TNullCodec() {
            MyTraits.Irreversible = true;
            MyTraits.SizeOnDecodeMultiplier = 0;
            MyTraits.SizeOnEncodeMultiplier = 0;
        }

        TString GetName() const override {
            return "null";
        }

        ui8 Encode(TStringBuf, TBuffer& out) const override {
            out.Clear();
            return 0;
        }

        void Decode(TStringBuf, TBuffer& out) const override {
            out.Clear();
        }

    protected:
        void DoLearn(ISequenceReader&) override {
        }
    };

    class TPipelineCodec: public ICodec {
        typedef TVector<TCodecPtr> TPipeline;

        TPipeline Pipeline;
        TString MyName;

    public:
        explicit TPipelineCodec(TCodecPtr c0 = nullptr, TCodecPtr c1 = nullptr, TCodecPtr c2 = nullptr, TCodecPtr c3 = nullptr) {
            MyTraits.PreservesPrefixGrouping = true;
            AddCodec(c0);
            AddCodec(c1);
            AddCodec(c2);
            AddCodec(c3);
        }

        TString GetName() const override {
            return MyName;
        }

        ui8 Encode(TStringBuf in, TBuffer& out) const override;
        void Decode(TStringBuf in, TBuffer& out) const override;

    public:
        /*
     * Add codecs in the following order:
     * uncompressed -> codec0 | codec1 | ... | codecN -> compressed
     */
        TPipelineCodec& AddCodec(TCodecPtr codec);

        bool AlreadyTrained() const override;
        void SetTrained(bool t) override;

    protected:
        void DoLearn(ISequenceReader& in) override {
            DoLearnX(in, 1);
        }

        void DoLearnX(ISequenceReader& in, double sampleSizeMult) override;
        void Save(IOutputStream* out) const override;
        void Load(IInputStream* in) override;
    };

}
