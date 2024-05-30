#include <library/cpp/testing/unittest/registar.h>
#include "cert_auth_utils.h"

namespace NKikimr {

Y_UNIT_TEST_SUITE(TCertificateAuthUtilsTest) {
    Y_UNIT_TEST(GenerateAndVerifyCertificates) {
        TCertAndKey ca = GenerateCA(TProps::AsCA());

        TCertAndKey server = GenerateSignedCert(ca, TProps::AsServer());
        VerifyCert(server.Certificate, ca.Certificate);

        TCertAndKey client = GenerateSignedCert(ca, TProps::AsClient());
        VerifyCert(client.Certificate, ca.Certificate);

        TCertAndKey clientServer = GenerateSignedCert(ca, TProps::AsClientServer());
        VerifyCert(clientServer.Certificate, ca.Certificate);
    }

    Y_UNIT_TEST(ClientCertAuthorizationParamsMatch) {
        {
            TDynamicNodeAuthorizationParams authParams;
            TDynamicNodeAuthorizationParams::TDistinguishedName dn;
            dn.AddRelativeDistinguishedName(TDynamicNodeAuthorizationParams::TRelativeDistinguishedName("C").AddValue("RU"))
            .AddRelativeDistinguishedName(TDynamicNodeAuthorizationParams::TRelativeDistinguishedName("ST").AddValue("MSK"))
            .AddRelativeDistinguishedName(TDynamicNodeAuthorizationParams::TRelativeDistinguishedName("L").AddValue("MSK"))
            .AddRelativeDistinguishedName(TDynamicNodeAuthorizationParams::TRelativeDistinguishedName("O").AddValue("YA"))
            .AddRelativeDistinguishedName(TDynamicNodeAuthorizationParams::TRelativeDistinguishedName("OU").AddValue("UtTest"))
            .AddRelativeDistinguishedName(TDynamicNodeAuthorizationParams::TRelativeDistinguishedName("CN").AddValue("localhost").AddSuffix(".yandex.ru"));
            authParams.AddCertSubjectDescription(dn);

            {
                TMap<TString, TString> subjectTerms;
                subjectTerms["C"] = "RU";
                subjectTerms["ST"] = "MSK";
                subjectTerms["L"] = "MSK";
                subjectTerms["O"] = "YA";
                subjectTerms["OU"] = "UtTest";
                subjectTerms["CN"] = "localhost";

                UNIT_ASSERT(authParams.IsSubjectDescriptionMatched(subjectTerms));
            }

            {
                TMap<TString, TString> subjectTerms;
                subjectTerms["C"] = "RU";
                subjectTerms["ST"] = "MSK";
                subjectTerms["L"] = "MSK";
                subjectTerms["O"] = "YA";
                subjectTerms["OU"] = "UtTest";
                subjectTerms["CN"] = "test.yandex.ru";

                UNIT_ASSERT(authParams.IsSubjectDescriptionMatched(subjectTerms));
            }

            {
                TMap<TString, TString> subjectTerms;
                subjectTerms["C"] = "RU";
                subjectTerms["ST"] = "MSK";
                subjectTerms["L"] = "MSK";
                subjectTerms["O"] = "YA";
                subjectTerms["OU"] = "UtTest";
                subjectTerms["CN"] = "test.yandex.ru";
                subjectTerms["ELSE"] = "WhatEver";

                UNIT_ASSERT(authParams.IsSubjectDescriptionMatched(subjectTerms));
            }

            {
                TMap<TString, TString> subjectTerms;
                subjectTerms["C"] = "WRONG";
                subjectTerms["ST"] = "MSK";
                subjectTerms["L"] = "MSK";
                subjectTerms["O"] = "YA";
                subjectTerms["OU"] = "UtTest";
                subjectTerms["CN"] = "test.yandex.ru";

                UNIT_ASSERT(!authParams.IsSubjectDescriptionMatched(subjectTerms));
            }

            {
                TMap<TString, TString> subjectTerms;
                subjectTerms["C"] = "RU";
                subjectTerms["ST"] = "MSK";
                subjectTerms["L"] = "MSK";
                subjectTerms["O"] = "YA";
                subjectTerms["OU"] = "UtTest";
                subjectTerms["CN"] = "test.not-yandex.ru";

                UNIT_ASSERT(!authParams.IsSubjectDescriptionMatched(subjectTerms));
            }

            {
                TMap<TString, TString> subjectTerms;
                //subjectTerms["C"] = "RU";
                subjectTerms["ST"] = "MSK";
                subjectTerms["L"] = "MSK";
                subjectTerms["O"] = "YA";
                subjectTerms["OU"] = "UtTest";
                subjectTerms["CN"] = "test.yandex.ru";

                UNIT_ASSERT(!authParams.IsSubjectDescriptionMatched(subjectTerms));
            }
        }
    }
}

} // namespace NKikimr
