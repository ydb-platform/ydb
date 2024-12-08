#include "yql_name_source.h"

#include <util/charset/utf8.h>

namespace NYdb {
    namespace NConsoleClient {

        using NThreading::MakeFuture;

        TFuture<TYQLNamesList> TYQLNameSource::GetNamesStartingWith(TStringBuf prefix) {
            return GetNames().Apply([prefix = ToLowerUTF8(TYQLName(prefix))](auto&& future) {
                TYQLNamesList names = future.GetValue();
                auto removed = std::ranges::remove_if(names, [&](const TYQLName& name) {
                    return !ToLowerUTF8(name).StartsWith(prefix);
                });
                names.erase(std::begin(removed), std::end(removed));
                return names;
            });
        }

        TYQLKeywordSource::TYQLKeywordSource(TYQLNamesList keywords)
            : Keywords(std::move(keywords))
        {
        }

        TFuture<TYQLNamesList> TYQLKeywordSource::GetNames() {
            return MakeFuture(Keywords);
        }

    } // namespace NConsoleClient
} // namespace NYdb
