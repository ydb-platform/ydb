#pragma once

#include <yql/essentials/public/udf/udf_registrator.h>

namespace NKnn {

struct TRemoteEmbedder {
    static NYql::NUdf::TStringRef Name() {
        return NYql::NUdf::TStringRef::Of("ApplyRemoteEmbedder");
    }

    static bool DeclareSignature(const NYql::NUdf::TStringRef& name, NYql::NUdf::TType* userType,
        NYql::NUdf::IFunctionTypeInfoBuilder& builder, bool typesOnly);
};

struct TRemoteEmbedderStream {
    using TTypeAwareMarker = bool;

    static NYql::NUdf::TStringRef Name() {
        return NYql::NUdf::TStringRef::Of("ApplyRemoteEmbedderStream");
    }

    static bool DeclareSignature(const NYql::NUdf::TStringRef& name, NYql::NUdf::TType* userType,
        NYql::NUdf::IFunctionTypeInfoBuilder& builder, bool typesOnly);
};

} // namespace NKnn
