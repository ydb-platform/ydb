#include "recursive_list.h"
#include "sys.h"

#include <util/string/join.h>

namespace NYdb::NConsoleClient {

using namespace NScheme;

namespace {

    TSchemeEntry ReplaceNameWithPath(const TSchemeEntry& entry, const TString& path) {
        TSchemeEntry result(entry);
        result.Name = path;
        return result;
    }

    TStatus RecursiveList(TVector<TSchemeEntry>& dst, TSchemeClient& client,
            const TString& path, const TRecursiveListSettings& settings, bool addSelf = false)
    {
        auto list = client.ListDirectory(path, settings.ListDirectorySettings_).ExtractValueSync();
        if (!list.IsSuccess()) {
            return list;
        }

        const auto& self = list.GetEntry();
        if (addSelf && settings.Filter_(self)) {
            dst.push_back(ReplaceNameWithPath(self, path));
        }

        for (const auto& child : list.GetChildren()) {
            if (settings.SkipSys_ && IsSystemObject(child)) {
                continue;
            }

            if (settings.Filter_(child)) {
                dst.push_back(ReplaceNameWithPath(child, Join('/', path, child.Name)));
            }

            switch (child.Type) {
                case ESchemeEntryType::SubDomain:
                case ESchemeEntryType::ColumnStore:
                case ESchemeEntryType::Directory: {
                    auto status = RecursiveList(dst, client, Join('/', path, child.Name), settings);
                    if (!status.IsSuccess()) {
                        return status;
                    }
                    break;
                }
                default:
                    break;
            }
        }

        return list;
    }

} // anonymous

TRecursiveListResult RecursiveList(TSchemeClient& client, const TString& path,
        const TRecursiveListSettings& settings, bool addSelf)
{
    TVector<TSchemeEntry> entries;
    auto status = RecursiveList(entries, client, path, settings, addSelf);
    return {entries, status};
}

}
