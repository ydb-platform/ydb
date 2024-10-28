#pragma once
#include <util/generic/string.h>

namespace NKikimr::NArrow::NAccessor {

class TGlobalConst {
public:
    static const inline TString SparsedDataAccessorName = "SPARSED";
    static const inline TString PlainDataAccessorName = "PLAIN";
};

}   // namespace NKikimr::NArrow::NAccessor
