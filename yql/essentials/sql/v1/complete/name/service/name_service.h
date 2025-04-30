#pragma once

#include <yql/essentials/sql/v1/complete/core/statement.h>

#include <library/cpp/threading/future/core/future.h>

#include <util/generic/vector.h>
#include <util/generic/string.h>
#include <util/generic/maybe.h>

namespace NSQLComplete {

    using NThreading::TFuture; // TODO(YQL-19747): remove

    struct TIndentifier {
        TString Indentifier;
    };

    struct TNamespaced {
        TString Namespace;
    };

    struct TKeyword {
        TString Content;
    };

    struct TPragmaName: TIndentifier {
        struct TConstraints: TNamespaced {};
    };

    struct TTypeName: TIndentifier {
        using TConstraints = std::monostate;
    };

    struct TFunctionName: TIndentifier {
        struct TConstraints: TNamespaced {};
    };

    struct THintName: TIndentifier {
        struct TConstraints {
            EStatementKind Statement;
        };
    };

    using TGenericName = std::variant<
        TKeyword,
        TPragmaName,
        TTypeName,
        TFunctionName,
        THintName>;

    struct TNameConstraints {
        TMaybe<TPragmaName::TConstraints> Pragma;
        TMaybe<TTypeName::TConstraints> Type;
        TMaybe<TFunctionName::TConstraints> Function;
        TMaybe<THintName::TConstraints> Hint;

        TGenericName Qualified(TGenericName unqualified) const;
        TGenericName Unqualified(TGenericName qualified) const;
        TVector<TGenericName> Qualified(TVector<TGenericName> unqualified) const;
        TVector<TGenericName> Unqualified(TVector<TGenericName> qualified) const;
    };

    struct TNameRequest {
        TVector<TString> Keywords;
        TNameConstraints Constraints;
        TString Prefix = "";
        size_t Limit = 128;

        bool IsEmpty() const {
            return Keywords.empty() &&
                   !Constraints.Pragma &&
                   !Constraints.Type &&
                   !Constraints.Function &&
                   !Constraints.Hint;
        }
    };

    struct TNameResponse {
        TVector<TGenericName> RankedNames;
    };

    class INameService: public TThrRefBase {
    public:
        using TPtr = TIntrusivePtr<INameService>;

        virtual TFuture<TNameResponse> Lookup(TNameRequest request) const = 0;
        virtual ~INameService() = default;
    };

} // namespace NSQLComplete
