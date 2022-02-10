 
#include <ydb/library/security/util.h>
 
#include <library/cpp/testing/unittest/registar.h> 
 
Y_UNIT_TEST_SUITE(Util) { 
    Y_UNIT_TEST(MaskTicket) { 
        TString ticket = "my_secret_abaabaabaaba"; 
        UNIT_ASSERT_VALUES_EQUAL(NKikimr::MaskTicket(ticket), "my_s****aaba (47A7C701)"); 
    } 
} 
