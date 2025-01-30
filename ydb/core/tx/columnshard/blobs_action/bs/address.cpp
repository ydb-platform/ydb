#include "address.h"
#include <util/string/builder.h>

namespace NKikimr::NOlap::NBlobOperations::NBlobStorage {

TString TBlobAddress::DebugString() const {
    return TStringBuilder() << "g=" << GroupId << ";c=" << ChannelId << ";";
}

}