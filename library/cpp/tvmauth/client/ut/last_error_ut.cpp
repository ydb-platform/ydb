#include <library/cpp/tvmauth/client/misc/last_error.h> 
 
#include <library/cpp/testing/unittest/registar.h> 
 
using namespace NTvmAuth; 
 
Y_UNIT_TEST_SUITE(LastError) { 
    Y_UNIT_TEST(common) { 
        TLastError le; 
 
        UNIT_ASSERT_VALUES_EQUAL("OK", 
                                 le.GetLastError(true)); 
        UNIT_ASSERT_VALUES_EQUAL("Internal client error: failed to collect last useful error message, please report this message to tvm-dev@yandex-team.ru", 
                                 le.GetLastError(false)); 
 
        UNIT_ASSERT_EXCEPTION_CONTAINS(le.ThrowLastError(), 
                                       TNonRetriableException, 
                                       "Internal client error: failed to collect last useful error message"); 
 
        le.ProcessError(TLastError::EType::Retriable, TLastError::EScope::PublicKeys, "err_re#1"); 
        UNIT_ASSERT_VALUES_EQUAL("PublicKeys: err_re#1", 
                                 le.GetLastError(false)); 
        le.ProcessError(TLastError::EType::Retriable, TLastError::EScope::PublicKeys, "err_re#2"); 
        UNIT_ASSERT_VALUES_EQUAL("PublicKeys: err_re#2", 
                                 le.GetLastError(false)); 
        le.ProcessError(TLastError::EType::NonRetriable, TLastError::EScope::PublicKeys, "err_nonre#3"); 
        UNIT_ASSERT_VALUES_EQUAL("PublicKeys: err_nonre#3", 
                                 le.GetLastError(false)); 
        le.ProcessError(TLastError::EType::NonRetriable, TLastError::EScope::PublicKeys, "err_nonre#4"); 
        UNIT_ASSERT_VALUES_EQUAL("PublicKeys: err_nonre#4", 
                                 le.GetLastError(false)); 
        le.ProcessError(TLastError::EType::Retriable, TLastError::EScope::PublicKeys, "err_re#5"); 
        UNIT_ASSERT_VALUES_EQUAL("PublicKeys: err_nonre#4", 
                                 le.GetLastError(false)); 
        UNIT_ASSERT_EXCEPTION_CONTAINS(le.ThrowLastError(), 
                                       TNonRetriableException, 
                                       "Failed to start TvmClient. Do not retry: PublicKeys: err_nonre#4"); 
 
        le.ProcessError(TLastError::EType::Retriable, TLastError::EScope::ServiceTickets, "err_re#6"); 
        UNIT_ASSERT_VALUES_EQUAL("PublicKeys: err_nonre#4", 
                                 le.GetLastError(false)); 
        le.ProcessError(TLastError::EType::Retriable, TLastError::EScope::ServiceTickets, "err_re#7"); 
        UNIT_ASSERT_VALUES_EQUAL("PublicKeys: err_nonre#4", 
                                 le.GetLastError(false)); 
        le.ProcessError(TLastError::EType::NonRetriable, TLastError::EScope::ServiceTickets, "err_nonre#8"); 
        UNIT_ASSERT_VALUES_EQUAL("ServiceTickets: err_nonre#8", 
                                 le.GetLastError(false)); 
 
        le.ClearError(TLastError::EScope::ServiceTickets); 
        UNIT_ASSERT_VALUES_EQUAL("PublicKeys: err_nonre#4", 
                                 le.GetLastError(false)); 
        le.ClearError(TLastError::EScope::PublicKeys); 
        UNIT_ASSERT_VALUES_EQUAL("Internal client error: failed to collect last useful error message, please report this message to tvm-dev@yandex-team.ru", 
                                 le.GetLastError(false)); 
    } 
} 
