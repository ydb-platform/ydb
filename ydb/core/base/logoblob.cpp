#include "logoblob.h"
#include <ydb/core/protos/base.pb.h>

namespace NKikimr {

TString TLogoBlobID::ToString() const {
    TString str;
    str.reserve(64);
    TStringOutput outStr(str);
    Out(outStr);
    return str;
}

void TLogoBlobID::Out(IOutputStream &o) const {
    o << '['
        << TabletID() << ':'
        << Generation() << ':'
        << Step() << ':'
        << Channel() << ':'
        << Cookie() << ':'
        << BlobSize() << ':'
        << PartId()
        << ']' ;
}

void TLogoBlobID::Out(IOutputStream &o, const TVector<TLogoBlobID> &vec) {
    o << "[ ";
    for (const auto &x : vec)
        o << x << ' ';
    o << "]";
}

static const char *SkipSpaces(const char *str) {
    while (str && *str && *str == ' ')
        ++str;
    return str;
}

#define PARSE_NUM(field_def, field_name_str, base) \
    field_def = strtoll(str, &endptr, base); \
    endptr = const_cast<char*>(SkipSpaces(endptr)); \
    if (!(endptr && *endptr == ':')) { \
        errorExplanation = "Can't find trailing ':' after " field_name_str; \
        return false; \
    } \
    str = endptr + 1;


bool TLogoBlobID::Parse(TLogoBlobID &out, const TString &buf, TString &errorExplanation) {
    const char *str = buf.data();
    char *endptr = nullptr;

    str = SkipSpaces(str);
    if (*str != '[') {
        errorExplanation = "Value doesn't start with '['";
        return false;
    }
    ++str;
    PARSE_NUM(const ui64 tabletID, "tablet id", 10);
    PARSE_NUM(const ui64 gen, "generation", 10);
    PARSE_NUM(const ui64 step, "step", 10);
    PARSE_NUM(const ui64 channel, "channel", 10);
    PARSE_NUM(const ui64 cookie, "cookie", 10);
    PARSE_NUM(const ui64 blobSize, "blob size", 10);

    const ui64 partid = strtoll(str, &endptr, 10);

    str = SkipSpaces(endptr);
    if (!(str && *str && *str == ']')) {
        errorExplanation = "Can't find trailing ']' after part id";
        return false;
    }
    str = SkipSpaces(str + 1);
    if (!(str && *str == '\0')) {
        errorExplanation = "Garbage after ']'";
        return false;
    }

    if (partid)
        out = TLogoBlobID(tabletID, gen, step, channel, blobSize, cookie, partid);
    else
        out = TLogoBlobID(tabletID, gen, step, channel, blobSize, cookie);

    return true;
}

TLogoBlobID LogoBlobIDFromLogoBlobID(const NKikimrProto::TLogoBlobID &proto) {
    return TLogoBlobID(proto.GetRawX1(), proto.GetRawX2(), proto.GetRawX3());
}

void LogoBlobIDFromLogoBlobID(const TLogoBlobID &id, NKikimrProto::TLogoBlobID *proto) {
    const ui64* raw = id.GetRaw();
    proto->SetRawX1(raw[0]);
    proto->SetRawX2(raw[1]);
    proto->SetRawX3(raw[2]);
}

void LogoBlobIDVectorFromLogoBlobIDRepeated(
            TVector<TLogoBlobID> *to,
            const ::google::protobuf::RepeatedPtrField<NKikimrProto::TLogoBlobID> &proto) {
    to->reserve(proto.size());
    to->clear();
    for (const auto &x : proto)
        to->emplace_back(LogoBlobIDFromLogoBlobID(x));
}

}
