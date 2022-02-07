#include <library/cpp/tvmauth/src/rw/keys.h>

#include <library/cpp/string_utils/base64/base64.h>

#include <util/generic/yexception.h>

using namespace NTvmAuth;

const TString DATA = "my magic data";

int main(int, char**) {
    const NRw::TKeyPair pair = NRw::GenKeyPair(1024);
    const NRw::TRwPrivateKey priv(pair.Private, 0);
    const NRw::TRwPublicKey pub(pair.Public);

    Cout << "data='" << DATA << "'."
         << "private='" << Base64Encode(pair.Private) << "'."
         << "public='" << Base64Encode(pair.Public) << "'.";

    TString sign;
    try {
        sign = priv.SignTicket(DATA);
        Cout << "sign='" << Base64Encode(sign) << "'.";
        Y_ENSURE(pub.CheckSign(DATA, sign));
    } catch (const std::exception& e) {
        Cout << "what='" << e.what() << "'" << Endl;
        return 1;
    }
    Cout << Endl;

    return 0;
}
