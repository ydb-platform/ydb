#include <library/cpp/tvmauth/client/misc/utils.h> 
 
#include <library/cpp/testing/unittest/registar.h>
 
Y_UNIT_TEST_SUITE(UtilsTest) { 
    using namespace NTvmAuth; 
 
    Y_UNIT_TEST(ParseDstMap) { 
        using TMap = NTvmAuth::NTvmApi::TClientSettings::TDstMap; 
        UNIT_ASSERT_EQUAL(TMap(), NUtils::ParseDstMap("")); 
        UNIT_ASSERT_EXCEPTION(NUtils::ParseDstMap(";"), TFromStringException); 
        UNIT_ASSERT_EXCEPTION(NUtils::ParseDstMap(":"), TFromStringException); 
        UNIT_ASSERT_EXCEPTION(NUtils::ParseDstMap("3;"), TFromStringException); 
        UNIT_ASSERT_EXCEPTION(NUtils::ParseDstMap("3:foo;"), TFromStringException); 
 
        UNIT_ASSERT_EQUAL(TMap({ 
                              {"foo", 3}, 
                          }), 
                          NUtils::ParseDstMap("foo:3")); 
        UNIT_ASSERT_EQUAL(TMap({ 
                              {"foo", 3}, 
                              {"bar", 17}, 
                          }), 
                          NUtils::ParseDstMap("foo:3;bar:17;")); 
    } 
 
    Y_UNIT_TEST(ParseDstVector) { 
        using TVector = NTvmAuth::NTvmApi::TClientSettings::TDstVector; 
        UNIT_ASSERT_EQUAL(TVector(), NUtils::ParseDstVector("")); 
        UNIT_ASSERT_EXCEPTION_CONTAINS(NUtils::ParseDstVector(";"), 
                                       yexception, 
                                       "Cannot parse empty string as number"); 
        UNIT_ASSERT_EXCEPTION_CONTAINS(NUtils::ParseDstVector(":"), 
                                       yexception, 
                                       "Unexpected symbol"); 
        UNIT_ASSERT_EXCEPTION_CONTAINS(NUtils::ParseDstVector("3:foo;"), 
                                       yexception, 
                                       "Unexpected symbol"); 
        UNIT_ASSERT_EXCEPTION_CONTAINS(NUtils::ParseDstVector("foo:3;"), 
                                       yexception, 
                                       "Unexpected symbol"); 
 
        UNIT_ASSERT_EQUAL(TVector(1, 3), 
                          NUtils::ParseDstVector("3")); 
        UNIT_ASSERT_EQUAL(TVector({3, 17}), 
                          NUtils::ParseDstVector("3;17;")); 
    } 
 
    Y_UNIT_TEST(ToHex) { 
        UNIT_ASSERT_VALUES_EQUAL("", NUtils::ToHex("")); 
        UNIT_ASSERT_VALUES_EQUAL("61", NUtils::ToHex("a")); 
        UNIT_ASSERT_VALUES_EQUAL( 
            "6C6B787A6E7620736C6A6876627761656220", 
            NUtils::ToHex("lkxznv sljhvbwaeb ")); 
    } 
 
    Y_UNIT_TEST(CheckBbEnvOverriding) { 
        UNIT_ASSERT(NUtils::CheckBbEnvOverriding(EBlackboxEnv::Prod, EBlackboxEnv::Prod)); 
        UNIT_ASSERT(NUtils::CheckBbEnvOverriding(EBlackboxEnv::Prod, EBlackboxEnv::ProdYateam)); 
        UNIT_ASSERT(!NUtils::CheckBbEnvOverriding(EBlackboxEnv::Prod, EBlackboxEnv::Test)); 
        UNIT_ASSERT(!NUtils::CheckBbEnvOverriding(EBlackboxEnv::Prod, EBlackboxEnv::TestYateam)); 
        UNIT_ASSERT(!NUtils::CheckBbEnvOverriding(EBlackboxEnv::Prod, EBlackboxEnv::Stress)); 
 
        UNIT_ASSERT(NUtils::CheckBbEnvOverriding(EBlackboxEnv::ProdYateam, EBlackboxEnv::Prod)); 
        UNIT_ASSERT(NUtils::CheckBbEnvOverriding(EBlackboxEnv::ProdYateam, EBlackboxEnv::ProdYateam)); 
        UNIT_ASSERT(!NUtils::CheckBbEnvOverriding(EBlackboxEnv::ProdYateam, EBlackboxEnv::Test)); 
        UNIT_ASSERT(!NUtils::CheckBbEnvOverriding(EBlackboxEnv::ProdYateam, EBlackboxEnv::TestYateam)); 
        UNIT_ASSERT(!NUtils::CheckBbEnvOverriding(EBlackboxEnv::ProdYateam, EBlackboxEnv::Stress)); 
 
        UNIT_ASSERT(NUtils::CheckBbEnvOverriding(EBlackboxEnv::Test, EBlackboxEnv::Prod)); 
        UNIT_ASSERT(NUtils::CheckBbEnvOverriding(EBlackboxEnv::Test, EBlackboxEnv::ProdYateam)); 
        UNIT_ASSERT(NUtils::CheckBbEnvOverriding(EBlackboxEnv::Test, EBlackboxEnv::Test)); 
        UNIT_ASSERT(NUtils::CheckBbEnvOverriding(EBlackboxEnv::Test, EBlackboxEnv::TestYateam)); 
        UNIT_ASSERT(NUtils::CheckBbEnvOverriding(EBlackboxEnv::Test, EBlackboxEnv::Stress)); 
 
        UNIT_ASSERT(!NUtils::CheckBbEnvOverriding(EBlackboxEnv::TestYateam, EBlackboxEnv::Prod)); 
        UNIT_ASSERT(!NUtils::CheckBbEnvOverriding(EBlackboxEnv::TestYateam, EBlackboxEnv::ProdYateam)); 
        UNIT_ASSERT(NUtils::CheckBbEnvOverriding(EBlackboxEnv::TestYateam, EBlackboxEnv::Test)); 
        UNIT_ASSERT(NUtils::CheckBbEnvOverriding(EBlackboxEnv::TestYateam, EBlackboxEnv::TestYateam)); 
        UNIT_ASSERT(!NUtils::CheckBbEnvOverriding(EBlackboxEnv::TestYateam, EBlackboxEnv::Stress)); 
 
        UNIT_ASSERT(!NUtils::CheckBbEnvOverriding(EBlackboxEnv::Stress, EBlackboxEnv::Prod)); 
        UNIT_ASSERT(!NUtils::CheckBbEnvOverriding(EBlackboxEnv::Stress, EBlackboxEnv::ProdYateam)); 
        UNIT_ASSERT(!NUtils::CheckBbEnvOverriding(EBlackboxEnv::Stress, EBlackboxEnv::Test)); 
        UNIT_ASSERT(!NUtils::CheckBbEnvOverriding(EBlackboxEnv::Stress, EBlackboxEnv::TestYateam)); 
        UNIT_ASSERT(NUtils::CheckBbEnvOverriding(EBlackboxEnv::Stress, EBlackboxEnv::Stress)); 
    } 
} 
