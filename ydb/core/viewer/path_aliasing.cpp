#include "path_aliasing.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/path.h>
#include <ydb/core/path_aliasing/context/path_context.h>

namespace NKikimr::NViewer {

    TConclusion<TString> ResolveViewerSchemaPath(const NPathAliasing::TPathContext* context, const TString& logicalPath) {
        if (!context || context->Empty() || logicalPath.empty()) {
            return logicalPath;
        }
        TString candidate = CanonizePath(logicalPath);
        if (candidate.empty()) {
            candidate = "/";
        }
        auto result = context->NormalizePath(candidate);
        if (result.IsFail()) {
            return result.GetError();
        }
        return result->Outcome == NPathAliasing::EPathRewriteOutcome::Rewritten
                   ? std::move(result.DetachResult().Path)
                   : logicalPath;
    }

    TConclusion<TString> ResolveViewerSchemaPath(const TAppData& appData, const TString& logicalPath) {
        if (!appData.PathNormalizer || appData.PathNormalizer->Empty()) {
            return logicalPath;
        }
        const NPathAliasing::TPathContext context(*appData.PathNormalizer, Nothing());
        return ResolveViewerSchemaPath(&context, logicalPath);
    }

} // namespace NKikimr::NViewer
