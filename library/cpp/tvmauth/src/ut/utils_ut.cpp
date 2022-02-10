#include <library/cpp/tvmauth/src/utils.h> 
 
#include <library/cpp/testing/unittest/registar.h>
 
#include <util/generic/maybe.h> 
 
Y_UNIT_TEST_SUITE(UtilsTestSuite) {
    static const TString VALID_SERVICE_TICKET_1 = "3:serv:CBAQ__________9_IhkI5QEQHBoIYmI6c2VzczEaCGJiOnNlc3My:WUPx1cTf05fjD1exB35T5j2DCHWH1YaLJon_a4rN-D7JfXHK1Ai4wM4uSfboHD9xmGQH7extqtlEk1tCTCGm5qbRVloJwWzCZBXo3zKX6i1oBYP_89WcjCNPVe1e8jwGdLsnu6PpxL5cn0xCksiStILH5UmDR6xfkJdnmMG94o8"; 
    static const TString EXPIRED_SERVICE_TICKET = "3:serv:CBAQACIZCOUBEBwaCGJiOnNlc3MxGghiYjpzZXNzMg:IwfMNJYEqStY_SixwqJnyHOMCPR7-3HHk4uylB2oVRkthtezq-OOA7QizDvx7VABLs_iTlXuD1r5IjufNei_EiV145eaa3HIg4xCdJXCojMexf2UYJz8mF2b0YzFAy6_KWagU7xo13CyKAqzJuQf5MJcSUf0ecY9hVh36cJ51aw"; 
    using namespace NTvmAuth; 

    Y_UNIT_TEST(base64Test) {
        UNIT_ASSERT_VALUES_EQUAL("-hHx", NUtils::Bin2base64url("\xfa\x11\xf1"));
        UNIT_ASSERT_VALUES_EQUAL("-hHx_g", NUtils::Bin2base64url("\xfa\x11\xf1\xfe"));
        UNIT_ASSERT_VALUES_EQUAL("-hHx_v8", NUtils::Bin2base64url("\xfa\x11\xf1\xfe\xff"));

        UNIT_ASSERT_VALUES_EQUAL("", NUtils::Base64url2bin("hHx++"));
        UNIT_ASSERT_VALUES_EQUAL("", NUtils::Base64url2bin("&*^"));
        UNIT_ASSERT_VALUES_EQUAL("", NUtils::Base64url2bin(""));
        UNIT_ASSERT_VALUES_EQUAL("", NUtils::Bin2base64url(""));

        UNIT_ASSERT_VALUES_EQUAL("\xfa\x11\xf1", NUtils::Base64url2bin("-hHx"));
        UNIT_ASSERT_VALUES_EQUAL("\xfa\x11\xf1\xfe", NUtils::Base64url2bin("-hHx_g"));
        UNIT_ASSERT_VALUES_EQUAL("\xfa\x11\xf1\xfe", NUtils::Base64url2bin("-hHx_g="));
        UNIT_ASSERT_VALUES_EQUAL("\xfa\x11\xf1\xfe", NUtils::Base64url2bin("-hHx_g=="));
        UNIT_ASSERT_VALUES_EQUAL("\xfa\x11\xf1\xfe\xff", NUtils::Base64url2bin("-hHx_v8"));
        UNIT_ASSERT_VALUES_EQUAL("\xfa\x11\xf1\xfe\xff", NUtils::Base64url2bin("-hHx_v8="));

        UNIT_ASSERT_VALUES_EQUAL("SGVsbG8sIGV2ZXJ5Ym9keSE",
                                 NUtils::Bin2base64url(("Hello, everybody!"))); 
        UNIT_ASSERT_VALUES_EQUAL("Hello, everybody!",
                                 NUtils::Base64url2bin(("SGVsbG8sIGV2ZXJ5Ym9keSE"))); 
        UNIT_ASSERT_VALUES_EQUAL("VGhlIE1hZ2ljIFdvcmRzIGFyZSBTcXVlYW1pc2ggT3NzaWZyYWdl",
                                 NUtils::Bin2base64url(("The Magic Words are Squeamish Ossifrage"))); 
        UNIT_ASSERT_VALUES_EQUAL("The Magic Words are Squeamish Ossifrage",
                                 NUtils::Base64url2bin(("VGhlIE1hZ2ljIFdvcmRzIGFyZSBTcXVlYW1pc2ggT3NzaWZyYWdl"))); 
    }

    Y_UNIT_TEST(sign) {
        UNIT_ASSERT_VALUES_EQUAL("wkGfeuopf709ozPAeGcDMqtZXPzsWvuNJ1BL586dSug",
                                 NUtils::SignCgiParamsForTvm(NUtils::Base64url2bin("GRMJrKnj4fOVnvOqe-WyD1"), 
                                                             "1490000000", 
                                                             "13,19", 
                                                             "bb:sess,bb:sess2")); 

        UNIT_ASSERT_VALUES_EQUAL("HANDYrA4ApQMQ5cfSWZk_InHWJffoXAa57P_X_B5s4M",
                                 NUtils::SignCgiParamsForTvm(NUtils::Base64url2bin("GRMJrKnj4fOasvOqe-WyD1"), 
                                                             "1490000000", 
                                                             "13,19", 
                                                             "bb:sess,bb:sess2")); 

        UNIT_ASSERT_VALUES_EQUAL("T-M-3_qtjRM1dR_3hS1CRlHBTZRKK04doHXBJw-5VRk",
                                 NUtils::SignCgiParamsForTvm(NUtils::Base64url2bin("GRMJrKnj4fOasvOqe-WyD1"), 
                                                             "1490000001", 
                                                             "13,19", 
                                                             "bb:sess,bb:sess2")); 

        UNIT_ASSERT_VALUES_EQUAL("gwB6M_9Jij50ZADmlDMnoyLc6AhQmtq6MClgGzO1PBE",
                                 NUtils::SignCgiParamsForTvm(NUtils::Base64url2bin("GRMJrKnj4fOasvOqe-WyD1"), 
                                                             "1490000001", 
                                                             "13,19", 
                                                             "")); 
    }
 
    Y_UNIT_TEST(GetExpirationTime) { 
        UNIT_ASSERT(!NTvmAuth::NInternal::TCanningKnife::GetExpirationTime("3:aadasdasdasdas")); 
 
        UNIT_ASSERT(NTvmAuth::NInternal::TCanningKnife::GetExpirationTime(VALID_SERVICE_TICKET_1)); 
        UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(std::numeric_limits<time_t>::max()), 
                                 *NTvmAuth::NInternal::TCanningKnife::GetExpirationTime(VALID_SERVICE_TICKET_1)); 
 
        UNIT_ASSERT(NTvmAuth::NInternal::TCanningKnife::GetExpirationTime(EXPIRED_SERVICE_TICKET)); 
        UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(0), 
                                 *NTvmAuth::NInternal::TCanningKnife::GetExpirationTime(EXPIRED_SERVICE_TICKET)); 
    } 
 
    Y_UNIT_TEST(RemoveSignatureTest) { 
        UNIT_ASSERT_VALUES_EQUAL("1:serv:ASDkljbjhsdbfLJHABFJHBslfbsfjs:asdxcvbxcvniueliuweklsvds", 
                                 NUtils::RemoveTicketSignature("1:serv:ASDkljbjhsdbfLJHABFJHBslfbsfjs:asdxcvbxcvniueliuweklsvds")); 
        UNIT_ASSERT_VALUES_EQUAL("2:serv:ASDkljbjhsdbfLJHABFJHBslfbsfjs:asdxcvbxcvniueliuweklsvds", 
                                 NUtils::RemoveTicketSignature("2:serv:ASDkljbjhsdbfLJHABFJHBslfbsfjs:asdxcvbxcvniueliuweklsvds")); 
        UNIT_ASSERT_VALUES_EQUAL("4:serv:ASDkljbjhsdbfLJHABFJHBslfbsfjs:asdxcvbxcvniueliuweklsvds", 
                                 NUtils::RemoveTicketSignature("4:serv:ASDkljbjhsdbfLJHABFJHBslfbsfjs:asdxcvbxcvniueliuweklsvds")); 
        UNIT_ASSERT_VALUES_EQUAL("3.serv.ASDkljbjhsdbfLJHABFJHBslfbsfjs.asdxcvbxcvniueliuweklsvds", 
                                 NUtils::RemoveTicketSignature("3.serv.ASDkljbjhsdbfLJHABFJHBslfbsfjs.asdxcvbxcvniueliuweklsvds")); 
        UNIT_ASSERT_VALUES_EQUAL("3:serv:ASDkljbjhsdbfLJHABFJHBslfbsfjs:", 
                                 NUtils::RemoveTicketSignature("3:serv:ASDkljbjhsdbfLJHABFJHBslfbsfjs:asdxcvbxcvniueliuweklsvds")); 
        UNIT_ASSERT_VALUES_EQUAL("3:serv:ASDkljbjhsdbfLJHABFJHBslfbsfjs:", 
                                 NUtils::RemoveTicketSignature("3:serv:ASDkljbjhsdbfLJHABFJHBslfbsfjs:asdxcvbxcvniueliuweklsvds")); 
        UNIT_ASSERT_VALUES_EQUAL("3:serv:", 
                                 NUtils::RemoveTicketSignature("3:serv:ASDkljbjhsdbfLJHABFJHBslfbsfjs.asdxcvbxcvniueliuweklsvds")); 
        UNIT_ASSERT_VALUES_EQUAL("asdxcbvfgdsgfasdfxczvdsgfxcdvbcbvf", 
                                 NUtils::RemoveTicketSignature("asdxcbvfgdsgfasdfxczvdsgfxcdvbcbvf")); 
    } 
}
