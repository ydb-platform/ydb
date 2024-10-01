#include <library/cpp/testing/unittest/registar.h>
#include <unordered_map>
#include <vector>

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
            TCertificateAuthorizationParams::TDN dn;
            dn.AddRDN(TCertificateAuthorizationParams::TRDN("C").AddValue("RU"))
            .AddRDN(TCertificateAuthorizationParams::TRDN("ST").AddValue("MSK"))
            .AddRDN(TCertificateAuthorizationParams::TRDN("L").AddValue("MSK"))
            .AddRDN(TCertificateAuthorizationParams::TRDN("O").AddValue("YA"))
            .AddRDN(TCertificateAuthorizationParams::TRDN("OU").AddValue("UtTest").AddValue("OtherUnit"))
            .AddRDN(TCertificateAuthorizationParams::TRDN("CN").AddValue("localhost").AddSuffix(".yandex.ru"));
            TCertificateAuthorizationParams authParams(std::move(dn), std::nullopt);

            {
                std::unordered_map<TString, std::vector<TString>> subjectTerms;
                subjectTerms["C"].push_back("RU");
                subjectTerms["ST"].push_back("MSK");
                subjectTerms["L"].push_back("MSK");
                subjectTerms["O"].push_back("YA");
                subjectTerms["OU"].push_back("UtTest");
                subjectTerms["CN"].push_back("localhost");

                UNIT_ASSERT(authParams.CheckSubject(subjectTerms, {}));
            }

            {
                std::unordered_map<TString, std::vector<TString>> subjectTerms;
                subjectTerms["C"].push_back("RU");
                subjectTerms["ST"].push_back("MSK");
                subjectTerms["L"].push_back("MSK");
                subjectTerms["O"].push_back("YA");
                subjectTerms["OU"].push_back("UtTest");
                subjectTerms["OU"].push_back("OtherUnit");
                subjectTerms["CN"].push_back("localhost");

                UNIT_ASSERT(authParams.CheckSubject(subjectTerms, {}));
            }

            {
                std::unordered_map<TString, std::vector<TString>> subjectTerms;
                subjectTerms["C"].push_back("RU");
                subjectTerms["ST"].push_back("MSK");
                subjectTerms["L"].push_back("MSK");
                subjectTerms["O"].push_back("YA");
                subjectTerms["OU"].push_back("UtTest");
                subjectTerms["OU"].push_back("WrongUnit");
                subjectTerms["CN"].push_back("localhost");

                UNIT_ASSERT(!authParams.CheckSubject(subjectTerms, {}));
            }

            {
                std::unordered_map<TString, std::vector<TString>> subjectTerms;
                subjectTerms["C"].push_back("RU");
                subjectTerms["ST"].push_back("MSK");
                subjectTerms["L"].push_back("MSK");
                subjectTerms["O"].push_back("YA");
                subjectTerms["OU"].push_back("UtTest");
                subjectTerms["CN"].push_back("test.yandex.ru");

                UNIT_ASSERT(authParams.CheckSubject(subjectTerms, {}));
            }

            {
                std::unordered_map<TString, std::vector<TString>> subjectTerms;
                subjectTerms["C"].push_back("RU");
                subjectTerms["ST"].push_back("MSK");
                subjectTerms["L"].push_back("MSK");
                subjectTerms["O"].push_back("YA");
                subjectTerms["OU"].push_back("UtTest");
                subjectTerms["CN"].push_back("test.yandex.ru");
                subjectTerms["ELSE"].push_back("WhatEver");

                UNIT_ASSERT(authParams.CheckSubject(subjectTerms, {}));
            }

            {
                std::unordered_map<TString, std::vector<TString>> subjectTerms;
                subjectTerms["C"].push_back("WRONG");
                subjectTerms["ST"].push_back("MSK");
                subjectTerms["L"].push_back("MSK");
                subjectTerms["O"].push_back("YA");
                subjectTerms["OU"].push_back("UtTest");
                subjectTerms["CN"].push_back("test.yandex.ru");

                UNIT_ASSERT(!authParams.CheckSubject(subjectTerms, {}));
            }

            {
                std::unordered_map<TString, std::vector<TString>> subjectTerms;
                subjectTerms["C"].push_back("RU");
                subjectTerms["ST"].push_back("MSK");
                subjectTerms["L"].push_back("MSK");
                subjectTerms["O"].push_back("YA");
                subjectTerms["OU"].push_back("UtTest");
                subjectTerms["CN"].push_back("test.not-yandex.ru");

                UNIT_ASSERT(!authParams.CheckSubject(subjectTerms, {}));
            }

            {
                std::unordered_map<TString, std::vector<TString>> subjectTerms;
                //subjectTerms["C"] = "RU";
                subjectTerms["ST"].push_back("MSK");
                subjectTerms["L"].push_back("MSK");
                subjectTerms["O"].push_back("YA");
                subjectTerms["OU"].push_back("UtTest");
                subjectTerms["CN"].push_back("test.yandex.ru");

                UNIT_ASSERT(!authParams.CheckSubject(subjectTerms, {}));
            }

            {
                std::unordered_map<TString, std::vector<TString>> subjectTerms;
                //subjectTerms["C"] = "RU";
                subjectTerms["ST"].push_back("MSK");
                subjectTerms["L"].push_back("MSK");
                subjectTerms["O"].push_back("YA");
                subjectTerms["OU"].push_back("UtTest");
                subjectTerms["CN"].push_back("test.yandex.ru");

                UNIT_ASSERT(!authParams.CheckSubject(subjectTerms, {"test.yandex.ru"}));
            }
        }
    }
}

} // namespace NKikimr
