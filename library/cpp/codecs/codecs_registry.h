#pragma once

#include "codecs.h"
#include <util/string/cast.h>

namespace NCodecs {
    struct TNoCodecException : TCodecException {
        TNoCodecException(TStringBuf name) {
            (*this) << "unknown codec: " << name;
        }
    };

    struct ICodecFactory : TAtomicRefCount<ICodecFactory> {
        virtual ~ICodecFactory() = default;
        virtual TCodecPtr MakeCodec(TStringBuf name) const = 0;
        virtual TVector<TString> ListNames() const = 0;
    };

    typedef TIntrusivePtr<ICodecFactory> TCodecFactoryPtr;

    namespace NPrivate {
        template <typename TCodec>
        struct TInstanceFactory : ICodecFactory {
            TCodecPtr MakeCodec(TStringBuf) const override {
                return new TCodec;
            }

            TVector<TString> ListNames() const override {
                TVector<TString> vs;
                vs.push_back(ToString(TCodec::MyName()));
                return vs;
            }
        };

        class TCodecRegistry {
            using TRegistry = THashMap<TString, TIntrusivePtr<ICodecFactory>>;
            TRegistry Registry;

        public:
            using TFactoryPtr = TIntrusivePtr<ICodecFactory>;

            TCodecRegistry();

            void RegisterFactory(TFactoryPtr fac);

            TCodecPtr GetCodec(TStringBuf name) const;

            TVector<TString> GetCodecsList() const;
        };

    }

    void RegisterCodecFactory(TCodecFactoryPtr fact);

    template <typename TCodec>
    void RegisterCodec() {
        RegisterCodecFactory(new NPrivate::TInstanceFactory<TCodec>());
    }

}
