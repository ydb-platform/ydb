#include "name_service.h"

#include <yql/essentials/core/sql_types/normalize_name.h>

#include <util/charset/utf8.h>

namespace NSQLComplete {

    namespace {

        void SetPrefix(TString& name, TStringBuf delimeter, const TNamespaced& namespaced) {
            if (namespaced.Namespace.empty()) {
                return;
            }

            name.prepend(delimeter);
            name.prepend(namespaced.Namespace);
        }

        void FixPrefix(TString& name, TStringBuf delimeter, const TNamespaced& namespaced) {
            if (namespaced.Namespace.empty()) {
                return;
            }

            name.remove(0, namespaced.Namespace.size() + delimeter.size());
        }

    } // namespace

    TGenericName TNameConstraints::Qualified(TGenericName unqualified) const {
        return std::visit([&](auto&& name) -> TGenericName {
            using T = std::decay_t<decltype(name)>;
            if constexpr (std::is_same_v<T, TPragmaName>) {
                SetPrefix(name.Identifier, ".", *Pragma);
            } else if constexpr (std::is_same_v<T, TFunctionName>) {
                SetPrefix(name.Identifier, "::", *Function);
            } else if constexpr (std::is_same_v<T, TClusterName>) {
                SetPrefix(name.Identifier, ":", *Cluster);
            }
            return name;
        }, std::move(unqualified));
    }

    TGenericName TNameConstraints::Unqualified(TGenericName qualified) const {
        return std::visit([&](auto&& name) -> TGenericName {
            using T = std::decay_t<decltype(name)>;
            if constexpr (std::is_same_v<T, TPragmaName>) {
                FixPrefix(name.Identifier, ".", *Pragma);
            } else if constexpr (std::is_same_v<T, TFunctionName>) {
                FixPrefix(name.Identifier, "::", *Function);
            } else if constexpr (std::is_same_v<T, TClusterName>) {
                FixPrefix(name.Identifier, ":", *Cluster);
            }
            return name;
        }, std::move(qualified));
    }

    TVector<TGenericName> TNameConstraints::Qualified(TVector<TGenericName> unqualified) const {
        for (auto& name : unqualified) {
            name = Qualified(std::move(name));
        }
        return unqualified;
    }

    TVector<TGenericName> TNameConstraints::Unqualified(TVector<TGenericName> qualified) const {
        for (auto& name : qualified) {
            name = Unqualified(std::move(name));
        }
        return qualified;
    }

    TString LowerizeName(TStringBuf name) {
        return ToLowerUTF8(name);
    }

    TString NormalizeName(TStringBuf name) {
        TString normalized(name);
        TMaybe<NYql::TIssue> error = NYql::NormalizeName(NYql::TPosition(), normalized);
        if (!error.Empty()) {
            return LowerizeName(name);
        }
        return normalized;
    }

} // namespace NSQLComplete
