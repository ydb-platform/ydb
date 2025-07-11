#include "name_service.h"

#include <yql/essentials/sql/v1/complete/name/service/static/name_index.h>

namespace NSQLComplete {

    namespace {

        class TNameService: public INameService {
        public:
            explicit TNameService(TVector<TString> names)
                : Index_(BuildNameIndex(std::move(names), NormalizeName))
            {
            }

            NThreading::TFuture<TNameResponse> Lookup(const TNameRequest& request) const override {
                if (request.Constraints.IsEmpty()) {
                    return NThreading::MakeFuture<TNameResponse>({});
                }

                TVector<TStringBuf> filtered =
                    FilteredByPrefix(request.Prefix, Index_, NormalizeName);
                filtered.crop(request.Limit);

                return NThreading::MakeFuture<TNameResponse>({
                    .RankedNames = Transform(filtered),
                });
            }

        private:
            static TVector<TGenericName> Transform(TVector<TStringBuf> names) {
                TVector<TGenericName> generic;
                generic.reserve(names.size());
                for (TStringBuf name : names) {
                    generic.emplace_back(Transform(name));
                }
                return generic;
            }

            static TGenericName Transform(TStringBuf name) {
                TBindingName unknown;
                unknown.Identifier = name;
                return unknown;
            }

            TNameIndex Index_;
        };

    } // namespace

    INameService::TPtr MakeBindingNameService(TVector<TString> names) {
        return new TNameService(std::move(names));
    }

} // namespace NSQLComplete
