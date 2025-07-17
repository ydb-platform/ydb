#include "documentation.h"

#include <yql/essentials/sql/v1/complete/name/service/name_service.h>

namespace NSQLComplete {

    namespace {

        class TDocumentation: public IDocumentation {
        public:
            explicit TDocumentation(TDocByNormalizedNameMap docs)
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

    } // namespace

    IDocumentation::TPtr MakeStaticDocumentation(TDocByNormalizedNameMap docs) {
        return new TDocumentation(std::move(docs));
    }

} // namespace NSQLComplete
