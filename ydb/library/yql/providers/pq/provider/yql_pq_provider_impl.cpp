#include "yql_pq_provider_impl.h"

namespace NYql {

TString MakeTopicDisplayName(TStringBuf cluster, TStringBuf path) {
    return TString::Join(cluster, ".`", path, "`");
}

} // namespace NYql
