#include "traceid.h"
#include <util/random/random.h>
#include <util/stream/str.h>

namespace NKikimr {
namespace NTracing {

TTraceID::TTraceID()
    : CreationTime(0)
    , RandomID(0)
{}

TTraceID::TTraceID(ui64 randomId, ui64 creationTime)
    : CreationTime(creationTime)
    , RandomID(randomId)
{}

TTraceID TTraceID::GenerateNew() {
    return TTraceID(RandomNumber<ui64>(), TInstant::Now().MicroSeconds());
}

TString TTraceID::ToString() const {
    TString str;
    str.reserve(128);
    TStringOutput outStr(str);
    Out(outStr);
    return str;
}

void TTraceID::Out(IOutputStream &o) const {
    o << "[ID: " << RandomID << ", " << "Created: " << TInstant::MicroSeconds(CreationTime).ToRfc822StringLocal() << "]";
}

bool TTraceID::operator<(const TTraceID &x) const {
    return CreationTime != x.CreationTime ? CreationTime < x.CreationTime : RandomID < x.RandomID;
}

bool TTraceID::operator>(const TTraceID &x) const {
    return (x < *this);
}

bool TTraceID::operator<=(const TTraceID &x) const {
    return CreationTime != x.CreationTime ? CreationTime < x.CreationTime : RandomID <= x.RandomID;
}

bool TTraceID::operator>=(const TTraceID &x) const {
    return (x <= *this);
}

bool TTraceID::operator==(const TTraceID &x) const {
    return CreationTime == x.CreationTime && RandomID == x.RandomID;
}

bool TTraceID::operator!=(const TTraceID &x) const {
    return CreationTime != x.CreationTime || RandomID != x.RandomID;
}

void TraceIDFromTraceID(const TTraceID& src, NKikimrTracing::TTraceID* dest) {
    Y_DEBUG_ABORT_UNLESS(dest);
    dest->SetCreationTime(src.CreationTime);
    dest->SetRandomID(src.RandomID);
}

TTraceID TraceIDFromTraceID(const NKikimrTracing::TTraceID &proto) {
    return TTraceID(proto.GetRandomID(), proto.GetCreationTime());
}

}
}
