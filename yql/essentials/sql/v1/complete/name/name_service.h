#pragma once

#include <library/cpp/threading/future/core/future.h>

#include <util/generic/vector.h>
#include <util/generic/string.h>

namespace NSQLComplete {

    using NThreading::TFuture;

    struct TIndentifier {
        TString Indentifier;
    };

    struct TTypeName: TIndentifier {
        using TConstraints = std::monostate;
    };

    using TGenericName = std::variant<
        TTypeName>;

    struct TNameRequest {
        struct {
            std::optional<TTypeName::TConstraints> TypeName;
        } Constraints;
        TString Prefix = "";
        size_t Limit = 128;

        bool IsEmpty() const {
            return !Constraints.TypeName;
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
