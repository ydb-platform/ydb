#pragma once

#include <library/cpp/threading/future/core/future.h>

#include <util/generic/vector.h>
#include <util/generic/string.h>

namespace NSQLComplete {

    using NThreading::TFuture;

    struct TIndentifier {
        TString Indentifier;
    };

    struct TNamespaced {
        TString Namespace;
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

    using TGenericName = std::variant<
        TPragmaName,
        TTypeName,
        TFunctionName>;

    struct TNameRequest {
        struct {
            std::optional<TPragmaName::TConstraints> Pragma;
            std::optional<TTypeName::TConstraints> Type;
            std::optional<TFunctionName::TConstraints> Function;
        } Constraints;
        TString Prefix = "";
        size_t Limit = 128;

        bool IsEmpty() const {
            return !Constraints.Pragma &&
                   !Constraints.Type &&
                   !Constraints.Function;
        }
    };

    struct TNameResponse {
        TVector<TGenericName> RankedNames;
    };

    class INameService {
    public:
        using TPtr = THolder<INameService>;

        virtual TFuture<TNameResponse> Lookup(TNameRequest request) = 0;
        virtual ~INameService() = default;
    };

} // namespace NSQLComplete
