#include "zstd_dict_codec.h"

#include <library/cpp/packers/packers.h>

#include <util/generic/ptr.h>
#include <util/generic/refcount.h>
#include <util/generic/noncopyable.h>
#include <util/string/builder.h>
#include <util/system/src_location.h>
#include <util/ysaveload.h>

#define ZDICT_STATIC_LINKING_ONLY

#include <contrib/libs/zstd/include/zdict.h>
#include <contrib/libs/zstd/include/zstd.h>
#include <contrib/libs/zstd/include/zstd_errors.h>

// See IGNIETFERRO-320 for possible bugs

namespace NCodecs {
    class TZStdDictCodec::TImpl: public TAtomicRefCount<TZStdDictCodec::TImpl> {
        template <class T, size_t Deleter(T*)>
        class TPtrHolder : TMoveOnly {
            T* Ptr = nullptr;

        public:
            TPtrHolder() = default;

            TPtrHolder(T* dict)
                : Ptr(dict)
            {
            }

            T* Get() {
                return Ptr;
            }

            const T* Get() const {
                return Ptr;
            }

            void Reset(T* dict) {
                Dispose();
                Ptr = dict;
            }

            void Dispose() {
                if (Ptr) {
                    Deleter(Ptr);
                    Ptr = nullptr;
                }
            }

            ~TPtrHolder() {
                Dispose();
            }
        };

        using TCDict = TPtrHolder<ZSTD_CDict, ZSTD_freeCDict>;
        using TDDict = TPtrHolder<ZSTD_DDict, ZSTD_freeDDict>;
        using TCCtx = TPtrHolder<ZSTD_CCtx, ZSTD_freeCCtx>;
        using TDCtx = TPtrHolder<ZSTD_DCtx, ZSTD_freeDCtx>;

        using TSizePacker = NPackers::TPacker<ui64>;

    public:
        static const ui32 SampleSize = (1 << 22) * 5;

        explicit TImpl(ui32 comprLevel)
            : CompressionLevel(comprLevel)
        {
            const size_t zeroSz = TSizePacker().MeasureLeaf(0);
            Zero.Resize(zeroSz);
            TSizePacker().PackLeaf(Zero.data(), 0, zeroSz);
        }

        ui32 GetCompressionLevel() const {
            return CompressionLevel;
        }

        ui8 Encode(TStringBuf in, TBuffer& outbuf) const {
            outbuf.Clear();

            if (in.empty()) {
                return 0;
            }

            TSizePacker packer;

            const char* rawBeg = in.data();
            const size_t rawSz = in.size();

            const size_t szSz = packer.MeasureLeaf(rawSz);
            const size_t maxDatSz = ZSTD_compressBound(rawSz);

            outbuf.Resize(szSz + maxDatSz);
            packer.PackLeaf(outbuf.data(), rawSz, szSz);

            TCCtx ctx{CheckPtr(ZSTD_createCCtx(), __LOCATION__)};
            const size_t resSz = CheckSize(ZSTD_compress_usingCDict(
                                               ctx.Get(), outbuf.data() + szSz, maxDatSz, rawBeg, rawSz, CDict.Get()),
                                           __LOCATION__);

            if (resSz < rawSz) {
                outbuf.Resize(resSz + szSz);
            } else {
                outbuf.Resize(Zero.size() + rawSz);
                memcpy(outbuf.data(), Zero.data(), Zero.size());
                memcpy(outbuf.data() + Zero.size(), rawBeg, rawSz);
            }
            return 0;
        }

        void Decode(TStringBuf in, TBuffer& outbuf) const {
            outbuf.Clear();

            if (in.empty()) {
                return;
            }

            TSizePacker packer;

            const char* rawBeg = in.data();
            size_t rawSz = in.size();

            const size_t szSz = packer.SkipLeaf(rawBeg);
            ui64 datSz = 0;
            packer.UnpackLeaf(rawBeg, datSz);

            rawBeg += szSz;
            rawSz -= szSz;

            if (!datSz) {
                outbuf.Resize(rawSz);
                memcpy(outbuf.data(), rawBeg, rawSz);
            } else {
                //                size_t zSz = ZSTD_getDecompressedSize(rawBeg, rawSz);
                //                Y_ENSURE_EX(datSz == zSz, TCodecException() << datSz << " != " << zSz);
                outbuf.Resize(datSz);
                TDCtx ctx{CheckPtr(ZSTD_createDCtx(), __LOCATION__)};
                CheckSize(ZSTD_decompress_usingDDict(
                              ctx.Get(), outbuf.data(), outbuf.size(), rawBeg, rawSz, DDict.Get()),
                          __LOCATION__);
                outbuf.Resize(datSz);
            }
        }

        bool Learn(ISequenceReader& in, bool throwOnError) {
            TBuffer data;
            TVector<size_t> lens;

            data.Reserve(2 * SampleSize);
            TStringBuf r;
            while (in.NextRegion(r)) {
                if (!r) {
                    continue;
                }
                data.Append(r.data(), r.size());
                lens.push_back(r.size());
            }

            ZDICT_legacy_params_t params;
            memset(&params, 0, sizeof(params));
            params.zParams.compressionLevel = 1;
            params.zParams.notificationLevel = 1;
            Dict.Resize(Max<size_t>(1 << 20, data.Size() + 16 * lens.size()));

            if (!lens) {
                Dict.Reset();
            } else {
                size_t trainResult = ZDICT_trainFromBuffer_legacy(
                    Dict.data(), Dict.size(), data.Data(), const_cast<const size_t*>(&lens[0]), lens.size(), params);
                if (ZSTD_isError(trainResult)) {
                    if (!throwOnError) {
                        return false;
                    }
                    CheckSize(trainResult, __LOCATION__);
                }
                Dict.Resize(trainResult);
                Dict.ShrinkToFit();
            }
            InitContexts();
            return true;
        }

        void Save(IOutputStream* out) const {
            ::Save(out, Dict);
        }

        void Load(IInputStream* in) {
            ::Load(in, Dict);
            InitContexts();
        }

        void InitContexts() {
            CDict.Reset(CheckPtr(ZSTD_createCDict(Dict.data(), Dict.size(), CompressionLevel), __LOCATION__));
            DDict.Reset(CheckPtr(ZSTD_createDDict(Dict.data(), Dict.size()), __LOCATION__));
        }

        static size_t CheckSize(size_t sz, TSourceLocation loc) {
            if (ZSTD_isError(sz)) {
                ythrow TCodecException() << loc << " " << ZSTD_getErrorName(sz) << " (code " << (int)ZSTD_getErrorCode(sz) << ")";
            }
            return sz;
        }

        template <class T>
        static T* CheckPtr(T* t, TSourceLocation loc) {
            Y_ENSURE_EX(t, TCodecException() << loc << " "
                                             << "unexpected nullptr");
            return t;
        }

    private:
        ui32 CompressionLevel = 1;

        TBuffer Zero;
        TBuffer Dict;

        TCDict CDict;
        TDDict DDict;
    };

    TZStdDictCodec::TZStdDictCodec(ui32 comprLevel)
        : Impl(new TImpl(comprLevel))
    {
        MyTraits.NeedsTraining = true;
        MyTraits.SizeOnEncodeMultiplier = 2;
        MyTraits.SizeOnDecodeMultiplier = 10;
        MyTraits.RecommendedSampleSize = TImpl::SampleSize; // same as for solar
    }

    TZStdDictCodec::~TZStdDictCodec() {
    }

    TString TZStdDictCodec::GetName() const {
        return TStringBuilder() << MyName() << "-" << Impl->GetCompressionLevel();
    }

    ui8 TZStdDictCodec::Encode(TStringBuf in, TBuffer& out) const {
        return Impl->Encode(in, out);
    }

    void TZStdDictCodec::Decode(TStringBuf in, TBuffer& out) const {
        Impl->Decode(in, out);
    }

    void TZStdDictCodec::DoLearn(ISequenceReader& in) {
        Impl = new TImpl(Impl->GetCompressionLevel());
        Impl->Learn(in, true/*throwOnError*/);
    }

    bool TZStdDictCodec::DoTryToLearn(ISequenceReader& in) {
        Impl = new TImpl(Impl->GetCompressionLevel());
        return Impl->Learn(in, false/*throwOnError*/);
    }

    void TZStdDictCodec::Save(IOutputStream* out) const {
        Impl->Save(out);
    }

    void TZStdDictCodec::Load(IInputStream* in) {
        Impl->Load(in);
    }

    TVector<TString> TZStdDictCodec::ListCompressionNames() {
        TVector<TString> res;
        for (int i = 1; i <= ZSTD_maxCLevel(); ++i) {
            res.emplace_back(TStringBuilder() << MyName() << "-" << i);
        }
        return res;
    }

    int TZStdDictCodec::ParseCompressionName(TStringBuf name) {
        int c = 0;
        TryFromString(name.After('-'), c);
        Y_ENSURE_EX(name.Before('-') == MyName() && c > 0 && c <= ZSTD_maxCLevel(), TCodecException() << "invald codec name" << name);
        return c;
    }

}
