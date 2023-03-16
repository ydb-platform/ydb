#include "signer.h"
#include <ydb/core/fq/libs/hmac/hmac.h>

#include <util/generic/yexception.h>
#include <util/stream/file.h>
#include <util/string/builder.h>

namespace NFq {

TSigner::TSigner(const TString& hmacSecret)
    : HmacSecret(hmacSecret) {
}

TString TSigner::Sign(Type type, const TString& value) const {
    TStringBuilder b;
    b << static_cast<unsigned char>(type) << value;
    return HmacSha1Base64(b, HmacSecret);
}

void TSigner::Verify(Type type, const TString& value, const TString& signature) const {
    const auto expectedSignature = Sign(type, value);
    if (signature != expectedSignature) {
        ythrow yexception() << "Incorrect signature for value: " << value << ", signature: " << signature << ", expected signature: " << expectedSignature;
    }
}

TSigner::TPtr CreateSignerFromFile(const TString& secretFile) {
    return new TSigner(TFileInput(secretFile).ReadAll());
}

}
