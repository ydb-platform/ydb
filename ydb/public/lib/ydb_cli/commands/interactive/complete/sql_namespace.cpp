#include "sql_namespace.h"

#include <util/charset/utf8.h>

namespace NSQLComplete {

    using NThreading::MakeFuture;

    TFuture<TSqlNamesList> TSqlNamespace::GetNamesStartingWith(TStringBuf prefix) {
        return GetNames().Apply([prefix = ToLowerUTF8(TSqlName(prefix))](auto&& future) {
            TSqlNamesList names = future.GetValue();
            auto removed = std::ranges::remove_if(names, [&](const TSqlName& name) {
                return !ToLowerUTF8(name).StartsWith(prefix);
            });
            names.erase(std::begin(removed), std::end(removed));
            return names;
        });
    }

    TSqlKeywords::TSqlKeywords(TSqlNamesList keywords)
        : Keywords(std::move(keywords))
    {
    }

    TFuture<TSqlNamesList> TSqlKeywords::GetNames() {
        return MakeFuture(Keywords);
    }

} // namespace NSQLComplete
