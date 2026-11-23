#pragma once

#include <util/generic/hash_set.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/strbuf.h>

namespace NYql {

    class TGatewaysConfig;

    // Absent for a null config, a disabled policy, or a non-Nile operation;
    // an empty set permits no clusters.
    TMaybe<THashSet<TString>> BuildNileOperationPermissions(const TGatewaysConfig* config, TStringBuf userName, const TMaybe<TString>& attributes);

} // namespace NYql
