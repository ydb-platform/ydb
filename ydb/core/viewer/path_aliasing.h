#pragma once

#include <ydb/library/conclusion/result.h>

namespace NKikimr {
    struct TAppData;
    namespace NPathAliasing {
        class TPathContext;
    } // namespace NPathAliasing
} // namespace NKikimr

namespace NKikimr::NViewer {

    TConclusion<TString> ResolveViewerSchemaPath(const NPathAliasing::TPathContext* context, const TString& logicalPath);
    TConclusion<TString> ResolveViewerSchemaPath(const TAppData& appData, const TString& logicalPath);

} // namespace NKikimr::NViewer
