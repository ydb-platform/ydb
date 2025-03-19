#pragma once
#include <util/generic/string.h>

namespace NSQLFormat {

bool SqlFormatSimple(const TString& query, TString& formattedQuery, TString& error);

}
