#include "custom_metrics.h"

namespace NKikimr::NHttpProxy {

    TVector<std::pair<TString, TString>> BuildLabels(const TString& method, const THttpRequestContext& httpContext, const TString& name, bool setStreamPrefix) {
        if (setStreamPrefix) {
            if (method.empty()) {
                return {{"cloud", httpContext.CloudId}, {"folder", httpContext.FolderId},
                        {"database", httpContext.DatabaseId}, {"stream", httpContext.StreamName},
                        {"name", name}};
            }
            return {{"method", method}, {"cloud", httpContext.CloudId}, {"folder", httpContext.FolderId},
                    {"database", httpContext.DatabaseId}, {"stream", httpContext.StreamName},
                    {"name", name}};
        }
        if (method.empty()) {
            return {{"database", httpContext.DatabasePath}, {"cloud_id", httpContext.CloudId},
                    {"folder_id", httpContext.FolderId}, {"database_id", httpContext.DatabaseId},
                    {"topic", httpContext.StreamName}, {"name", name}};
        }
        return {{"database", httpContext.DatabasePath}, {"method", method}, {"cloud_id", httpContext.CloudId},
                {"folder_id", httpContext.FolderId}, {"database_id", httpContext.DatabaseId},
                {"topic", httpContext.StreamName}, {"name", name}};
    }

} // namespace NKikimr::NHttpProxy
