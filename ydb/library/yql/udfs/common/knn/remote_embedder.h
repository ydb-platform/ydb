#pragma once

#include <yql/essentials/public/udf/udf_registrator.h>

#include <util/generic/strbuf.h>

namespace NKnn {

void ValidateRemoteEmbedderHost(TStringBuf host);

struct TRemoteEmbedder {
    using TTypeAwareMarker = void;

    static NYql::NUdf::TStringRef Name() {
        return NYql::NUdf::TStringRef::Of("ApplyRemoteEmbedder");
    }

    static bool DeclareSignature(const NYql::NUdf::TStringRef& name, NYql::NUdf::TType* userType,
        NYql::NUdf::IFunctionTypeInfoBuilder& builder, bool typesOnly);
};

} // namespace NKnn
