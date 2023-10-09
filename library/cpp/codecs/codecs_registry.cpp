#include "codecs_registry.h"
#include "delta_codec.h"
#include "huffman_codec.h"
#include "pfor_codec.h"
#include "solar_codec.h"
#include "comptable_codec.h"
#include "zstd_dict_codec.h"

#include <library/cpp/blockcodecs/codecs.h>

#include <util/string/builder.h>
#include <util/string/cast.h>

namespace NCodecs {
    TCodecPtr ICodec::GetInstance(TStringBuf name) {
        return Singleton<NPrivate::TCodecRegistry>()->GetCodec(name);
    }

    TVector<TString> ICodec::GetCodecsList() {
        return Singleton<NPrivate::TCodecRegistry>()->GetCodecsList();
    }

    namespace NPrivate {
        void TCodecRegistry::RegisterFactory(TFactoryPtr fac) {
            TVector<TString> names = fac->ListNames();
            for (const auto& name : names) {
                Y_ABORT_UNLESS(!Registry.contains(name), "already has %s", name.data());
                Registry[name] = fac;
            }
        }

        TCodecPtr TCodecRegistry::GetCodec(TStringBuf name) const {
            using namespace NPrivate;

            if (!name || "none" == name) {
                return nullptr;
            }

            if (TStringBuf::npos == name.find(':')) {
                Y_ENSURE_EX(Registry.contains(name), TNoCodecException(name));
                return Registry.find(name)->second->MakeCodec(name);
            } else {
                TPipelineCodec* pipe = new TPipelineCodec;

                do {
                    TStringBuf v = name.NextTok(':');
                    pipe->AddCodec(GetCodec(v));
                } while (name);

                return pipe;
            }
        }

        TVector<TString> TCodecRegistry::GetCodecsList() const {
            using namespace NPrivate;
            TVector<TString> vs;
            vs.push_back("none");

            for (const auto& it : Registry) {
                vs.push_back(it.first);
            }

            Sort(vs.begin(), vs.end());
            return vs;
        }

        struct TSolarCodecFactory : ICodecFactory {
            TCodecPtr MakeCodec(TStringBuf name) const override {
                if (TSolarCodec::MyNameShortInt() == name) {
                    return new TSolarCodecShortInt();
                }
                if (TSolarCodec::MyName() == name) {
                    return new TSolarCodec();
                }
                if (name.EndsWith(TStringBuf("-a"))) {
                    return MakeCodecImpl<TAdaptiveSolarCodec>(name, name.SubStr(TSolarCodec::MyName().size()).Chop(2));
                } else {
                    return MakeCodecImpl<TSolarCodec>(name, name.SubStr(TSolarCodec::MyName().size()));
                }
            }

            template <class TCodecCls>
            TCodecPtr MakeCodecImpl(const TStringBuf& name, const TStringBuf& type) const {
                if (TStringBuf("-8k") == type) {
                    return new TCodecCls(1 << 13);
                }
                if (TStringBuf("-16k") == type) {
                    return new TCodecCls(1 << 14);
                }
                if (TStringBuf("-32k") == type) {
                    return new TCodecCls(1 << 15);
                }
                if (TStringBuf("-64k") == type) {
                    return new TCodecCls(1 << 16);
                }
                if (TStringBuf("-256k") == type) {
                    return new TCodecCls(1 << 18);
                }
                ythrow TNoCodecException(name);
            }

            TVector<TString> ListNames() const override {
                TVector<TString> vs;
                vs.push_back(ToString(TSolarCodec::MyName()));
                vs.push_back(ToString(TSolarCodec::MyName8k()));
                vs.push_back(ToString(TSolarCodec::MyName16k()));
                vs.push_back(ToString(TSolarCodec::MyName32k()));
                vs.push_back(ToString(TSolarCodec::MyName64k()));
                vs.push_back(ToString(TSolarCodec::MyName256k()));
                vs.push_back(ToString(TSolarCodec::MyName8kAdapt()));
                vs.push_back(ToString(TSolarCodec::MyName16kAdapt()));
                vs.push_back(ToString(TSolarCodec::MyName32kAdapt()));
                vs.push_back(ToString(TSolarCodec::MyName64kAdapt()));
                vs.push_back(ToString(TSolarCodec::MyName256kAdapt()));
                vs.push_back(ToString(TSolarCodec::MyNameShortInt()));
                return vs;
            }
        };

        struct TZStdDictCodecFactory : ICodecFactory {
            TCodecPtr MakeCodec(TStringBuf name) const override {
                return new TZStdDictCodec(TZStdDictCodec::ParseCompressionName(name));
            }

            TVector<TString> ListNames() const override {
                return TZStdDictCodec::ListCompressionNames();
            }
        };

        struct TCompTableCodecFactory : ICodecFactory {
            TCodecPtr MakeCodec(TStringBuf name) const override {
                if (TCompTableCodec::MyNameHQ() == name) {
                    return new TCompTableCodec(TCompTableCodec::Q_HIGH);
                } else if (TCompTableCodec::MyNameLQ() == name) {
                    return new TCompTableCodec(TCompTableCodec::Q_LOW);
                } else {
                    Y_ENSURE_EX(false, TNoCodecException(name));
                    return nullptr;
                }
            }

            TVector<TString> ListNames() const override {
                TVector<TString> vs;
                vs.push_back(ToString(TCompTableCodec::MyNameHQ()));
                vs.push_back(ToString(TCompTableCodec::MyNameLQ()));
                return vs;
            }
        };

        struct TBlockCodec : ICodec {
            const NBlockCodecs::ICodec* Codec;

            TBlockCodec(TStringBuf name)
                : Codec(NBlockCodecs::Codec(name))
            {
            }

            TString GetName() const override {
                return ToString(Codec->Name());
            }

            ui8 Encode(TStringBuf r, TBuffer& b) const override {
                Codec->Encode(r, b);
                return 0;
            }

            void Decode(TStringBuf r, TBuffer& b) const override {
                // TODO: throws exception that is not TCodecException
                Codec->Decode(r, b);
            }

        protected:
            void DoLearn(ISequenceReader&) override {
            }
        };

        struct TBlockCodecsFactory : ICodecFactory {
            using TRegistry = THashMap<TString, TCodecPtr>;
            TRegistry Registry;

            TBlockCodecsFactory() {
                for (TStringBuf codec : NBlockCodecs::ListAllCodecs()) {
                    Register(codec);
                }
            }

            void Register(TStringBuf name) {
                TCodecPtr p = Registry[name] = new TBlockCodec(name);
                Registry[p->GetName()] = p;
            }

            TCodecPtr MakeCodec(TStringBuf name) const override {
                if (!Registry.contains(name)) {
                    ythrow TNoCodecException(name);
                }
                return Registry.find(name)->second;
            }

            TVector<TString> ListNames() const override {
                TVector<TString> res;
                for (const auto& it : Registry) {
                    res.push_back(it.first);
                }
                return res;
            }
        };

        TCodecRegistry::TCodecRegistry() {
            RegisterFactory(new TInstanceFactory<TTrivialCodec>);
            RegisterFactory(new TInstanceFactory<TTrivialTrainableCodec>);
            RegisterFactory(new TInstanceFactory<THuffmanCodec>);
            RegisterFactory(new TInstanceFactory<TPForCodec<ui64, true>>);
            RegisterFactory(new TInstanceFactory<TPForCodec<ui32, true>>);
            RegisterFactory(new TSolarCodecFactory);
            RegisterFactory(new TZStdDictCodecFactory);
            RegisterFactory(new TCompTableCodecFactory);
            RegisterFactory(new TBlockCodecsFactory);
        }

    }

    void RegisterCodecFactory(TCodecFactoryPtr fact) {
        Singleton<NPrivate::TCodecRegistry>()->RegisterFactory(fact);
    }

}
