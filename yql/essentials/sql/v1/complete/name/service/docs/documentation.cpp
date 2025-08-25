#include "documentation.h"

#include <yql/essentials/sql/v1/complete/name/service/name_service.h>

namespace NSQLComplete {

    namespace {

        class TStaticDocumentation: public IDocumentation {
        public:
            explicit TStaticDocumentation(TDocByNormalizedNameMap docs)
                : Docs_(std::move(docs))
            {
            }

            TMaybe<TString> Lookup(TStringBuf name) const override {
                if (auto* ptr = Docs_.FindPtr(Normalized(name))) {
                    return *ptr;
                }
                return Nothing();
            }

        private:
            TString Normalized(TStringBuf name) const {
                return NormalizeName(name);
            }

            TDocByNormalizedNameMap Docs_;
        };

        class TReservedDocumentation: public IDocumentation {
        public:
            TReservedDocumentation(
                IDocumentation::TPtr primary, IDocumentation::TPtr fallback)
                : Primary_(std::move(primary))
                , Fallback_(std::move(fallback))
            {
            }

            TMaybe<TString> Lookup(TStringBuf name) const override {
                return Primary_->Lookup(name).Or([name, f = Fallback_] {
                    return f->Lookup(name);
                });
            }

        private:
            IDocumentation::TPtr Primary_;
            IDocumentation::TPtr Fallback_;
        };

    } // namespace

    IDocumentation::TPtr MakeStaticDocumentation(TDocByNormalizedNameMap docs) {
        return new TStaticDocumentation(std::move(docs));
    }

    IDocumentation::TPtr MakeReservedDocumentation(
        IDocumentation::TPtr primary, IDocumentation::TPtr fallback) {
        return new TReservedDocumentation(std::move(primary), std::move(fallback));
    }

} // namespace NSQLComplete
