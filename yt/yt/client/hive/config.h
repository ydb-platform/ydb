#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NHiveClient {

////////////////////////////////////////////////////////////////////////////////

struct TClusterDirectoryConfig
    : public virtual NYTree::TYsonStruct
{
    THashMap<std::string, NYTree::INodePtr> PerClusterConnectionConfig;

    REGISTER_YSON_STRUCT(TClusterDirectoryConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TClusterDirectoryConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveClient
