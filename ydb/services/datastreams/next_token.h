#pragma once

#include <ydb/core/protos/pqconfig.pb.h>
#include <library/cpp/string_utils/base64/base64.h>
#include <util/datetime/base.h>

namespace NKikimr::NDataStreams::V1 {

class TNextToken {
public:
static constexpr ui64 LIFETIME_MS = TDuration::Minutes(5).MilliSeconds();

TNextToken(const TString& nextToken): Expired{false}, Valid{true}
{
    try {
        TString decoded;
        Base64StrictDecode(nextToken, decoded);
        Valid = Proto.ParseFromString(decoded) && IsAlive(TInstant::Now().MilliSeconds());
        Expired = !IsAlive(TInstant::Now().MilliSeconds());
    } catch (std::exception&) {
        Valid = false;
    }
}

TNextToken(const TString& streamArn, ui32 alreadyRead, ui32 maxResults, ui64 creationTimestamp)
: Expired{false}, Valid{true} {
    Proto.SetStreamArn(streamArn);
    Proto.SetAlreadyRead(alreadyRead);
    Proto.SetMaxResults(maxResults);
    Proto.SetCreationTimestamp(creationTimestamp);
}

TString Serialize() const {
    TString data;
    bool result = Proto.SerializeToString(&data);
    Y_ABORT_UNLESS(result);
    TString encoded;
    Base64Encode(data, encoded);
    return encoded;
}

ui32 GetAlreadyRead() const {
    return Proto.GetAlreadyRead();
}

TString GetStreamArn() const {
    return Proto.GetStreamArn();
}

TString GetStreamName() const {
    return Proto.GetStreamArn();
}

ui32 GetMaxResults() const {
    return Proto.GetMaxResults();
}

ui64 GetCreationTimestamp() const {
    return Proto.GetCreationTimestamp();
}

bool IsAlive(ui64 now) const {
    return now >= GetCreationTimestamp() &&
        (now - GetCreationTimestamp()) < LIFETIME_MS;
}

bool IsExpired() const {
    return Expired;
}

bool IsValid() const {
    return Valid && GetStreamArn().size() > 0;
}

private:
bool Expired;
bool Valid;
NKikimrPQ::TYdsNextToken Proto;
};

} // namespace NKikimr::NDataStreams::V1
