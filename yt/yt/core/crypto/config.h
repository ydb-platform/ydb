#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NCrypto {

////////////////////////////////////////////////////////////////////////////////

//! Either an inlined value or a file reference.
class TPemBlobConfig
    : public NYTree::TYsonStruct
{
public:
    std::optional<TString> FileName;
    std::optional<TString> Value;

    TString LoadBlob() const;

    REGISTER_YSON_STRUCT(TPemBlobConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TPemBlobConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCrypto
