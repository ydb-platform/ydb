#pragma once

#include <util/generic/maybe.h>
#include <util/generic/buffer.h>
#include <util/generic/string.h>
#include <util/stream/mem.h>

class THttpInput;
struct TParsedHttpFull;

namespace NKikimr::NSQS {

class TAwsRequestSignV4 {
public:
    TAwsRequestSignV4(const TString& request);
    TAwsRequestSignV4(const THttpInput& input, const TParsedHttpFull& parsed, const TMaybe<TBuffer>& inputData);

    const TString& GetCanonicalRequest() const;

    const TString& GetStringToSign() const;

    const TString& GetParsedSignature() const;

    const TString& GetSigningTimestamp() const;

    const TString& GetRegion() const;

    const TString& GetAccessKeyId() const;

    TString CalcSignature(const TString& secretKey) const;

private:
    void Process(const THttpInput& input, const TParsedHttpFull& parsed, const TMaybe<TBuffer>& inputData);
    void ParseAuthorization(const THttpInput& input);
    void MakeCanonicalRequest(const THttpInput& input, const TParsedHttpFull& parsed, const TMaybe<TBuffer>& inputData);
    void MakeFinalStringToSign();

private:
    TString SignedHeadersList_;
    TString ParsedSignature_;
    TString CanonicalRequestStr_;
    TString FinalStringToSignStr_;
    TString StringToSign_;
    TString AwsTimestamp_; // e. g. 20130524T000000Z
    TString AccessKeyId_;
    TString AwsDate_; // YYYYMMDD
    TString AwsRegion_;
    TString AwsService_;
    TString AwsRequest_ = "aws4_request";
};

} // namespace NKikimr::NSQS
