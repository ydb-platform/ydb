#pragma once

#include <yt/yql/providers/yt/gateway/lib/downloader.h>
#include <yt/yql/providers/yt/common/yql_yt_settings.h>

#include <yt/yql/providers/yt/provider/yql_yt_gateway.h>


#include <yql/essentials/core/yql_type_annotation.h>
#include <yql/essentials/core/yql_udf_resolver.h>
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/mkql_node_visitor.h>
#include <yql/essentials/minikql/mkql_program_builder.h>

using namespace NKikimr::NMiniKQL;
using namespace NKikimr;

namespace NYql {

ITableDownloaderFunc MakeYtNativeFileDownloader(
    IYtGateway::TPtr gateway,
    const TString& sessionId,
    const TString& cluster,
    TYtSettings::TConstPtr settings,
    NYT::IClientPtr client,
    TTempFiles::TPtr tmpFiles
);

} // namespace NYql
