#include "message_counter.h"

#include <util/stream/str.h>

using namespace NBus;
using namespace NBus::NPrivate;

TMessageCounter::TMessageCounter()
    : BytesData(0)
    , BytesNetwork(0)
    , Count(0)
    , CountCompressed(0)
    , CountCompressionRequests(0)
{
}

TMessageCounter& TMessageCounter::operator+=(const TMessageCounter& that) {
    BytesData += that.BytesData;
    BytesNetwork += that.BytesNetwork;
    Count += that.Count;
    CountCompressed += that.CountCompressed;
    CountCompressionRequests += that.CountCompressionRequests;
    return *this;
}

TString TMessageCounter::ToString(bool reader) const {
    if (reader) {
        Y_ASSERT(CountCompressionRequests == 0);
    }

    TStringStream readValue;
    readValue << Count;
    if (CountCompressionRequests != 0 || CountCompressed != 0) {
        readValue << " (" << CountCompressed << " compr";
        if (!reader) {
            readValue << ", " << CountCompressionRequests << " compr reqs";
        }
        readValue << ")";
    }
    readValue << ", ";
    readValue << BytesData << "b";
    if (BytesNetwork != BytesData) {
        readValue << " (" << BytesNetwork << "b network)";
    }
    return readValue.Str();
}
