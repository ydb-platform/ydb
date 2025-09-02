#pragma once

#include <library/cpp/yson/node/node.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/stream/str.h>


namespace NYql {
    namespace NPureCalc {
        namespace NPrivate {
            NYT::TNode GetSchema(
                const TVector<TString>& fields,
                const TVector<TString>& optionalFields = {}
            );
        }
    }
}
