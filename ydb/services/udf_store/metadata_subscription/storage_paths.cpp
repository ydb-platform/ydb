#include "storage_paths.h"

#include <ydb/core/base/appdata.h>
#include <ydb/services/metadata/service.h>

#include <util/string/builder.h>

namespace NKikimr::NUdfStore {

TString GetUdfStorePrefix() {
    const TString path = NMetadata::NProvider::TServiceOperator::GetPath();
    Y_ABORT_UNLESS(path);
    return "/" + AppData()->TenantName + "/" + path + "/udf_store";
}

TString GetModulesTablePath() {
    return GetUdfStorePrefix() + "/modules";
}

TString GetModuleChunksTablePath() {
    return GetUdfStorePrefix() + "/module_chunks";
}

TString GetArtifactTablePath(const TString& cpuSpec) {
    return GetUdfStorePrefix() + "/artifacts/" + cpuSpec;
}

TString GetArtifactChunksTablePath(const TString& cpuSpec) {
    return GetUdfStorePrefix() + "/artifacts/" + cpuSpec + "_chunks";
}

TString NormalizeCpuSpec(TStringBuf triple, TStringBuf cpu) {
    auto sanitize = [](TStringBuf value) {
        TString result;
        result.reserve(value.size());
        for (char ch : value) {
            if ((ch >= 'a' && ch <= 'z')
                || (ch >= 'A' && ch <= 'Z')
                || (ch >= '0' && ch <= '9')
                || ch == '-' || ch == '_')
            {
                result.push_back(ch);
            } else {
                result.push_back('_');
            }
        }
        return result.empty() ? TString("unknown") : result;
    };
    return TStringBuilder()
        << sanitize(triple)
        << "__"
        << sanitize(cpu.empty() ? "generic" : cpu);
}

} // namespace NKikimr::NUdfStore
