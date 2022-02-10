#include "common.h" 
 
#include <library/cpp/tvmauth/client/misc/async_updater.h> 
 
#include <library/cpp/tvmauth/unittest.h> 
 
#include <library/cpp/testing/unittest/registar.h> 
 
using namespace NTvmAuth; 
 
Y_UNIT_TEST_SUITE(AsyncUpdater) { 
    static const TString SRV_TICKET = "3:serv:CBAQ__________9_IgYIexCUkQY:GioCM49Ob6_f80y6FY0XBVN4hLXuMlFeyMvIMiDuQnZkbkLpRpQOuQo5YjWoBjM0Vf-XqOm8B7xtrvxSYHDD7Q4OatN2l-Iwg7i71lE3scUeD36x47st3nd0OThvtjrFx_D8mw_c0GT5KcniZlqq1SjhLyAk1b_zJsx8viRAhCU"; 
    static const TString PROD_TICKET = "3:user:CAsQ__________9_Gg4KAgh7EHsg0oXYzAQoAA:N8PvrDNLh-5JywinxJntLeQGDEHBUxfzjuvB8-_BEUv1x9CALU7do8irDlDYVeVVDr4AIpR087YPZVzWPAqmnBuRJS0tJXekmDDvrivLnbRrzY4IUXZ_fImB0fJhTyVetKv6RD11bGqnAJeDpIukBwPTbJc_EMvKDt8V490CJFw"; 
    static const TString TEST_TICKET = "3:user:CA0Q__________9_Gg4KAgh7EHsg0oXYzAQoAQ:FSADps3wNGm92Vyb1E9IVq5M6ZygdGdt1vafWWEhfDDeCLoVA-sJesxMl2pGW4OxJ8J1r_MfpG3ZoBk8rLVMHUFrPa6HheTbeXFAWl8quEniauXvKQe4VyrpA1SPgtRoFqi5upSDIJzEAe1YRJjq1EClQ_slMt8R0kA_JjKUX54"; 
    static const TString PROD_YATEAM_TICKET = "3:user:CAwQ__________9_Gg4KAgh7EHsg0oXYzAQoAg:M9dEFEWHLHXiL7brCsyfYlm254PE6VeshUjI62u2qMDRzt6-0jAoJTIdDiogerItht1YFYSn8fSqmMf23_rueGj-wkmvyNzbcBSk3jtK2U5sai_W0bK6OwukR9tzWzi1Gcgg9DrNEeIKFvs1EBqYCF4mPHWo5bgk0CR580Cgit4"; 
    static const TString TEST_YATEAM_TICKET = "3:user:CA4Q__________9_Gg4KAgh7EHsg0oXYzAQoAw:IlaV3htk3jYrviIOz3k3Dfwz7p-bYYpbrgdn53GiUrMGdrT9eobHeuzNvPLrWB0yuYZAD46C3MGxok4GGmHhT73mki4XOCX8yWT4jW_hzcHBik1442tjWwh8IWqV_7q5j5496suVuLWjnZORWbb7I-2iwdIlU1BUiDfhoAolCq8"; 
    static const TString STRESS_TICKET = "3:user:CA8Q__________9_Gg4KAgh7EHsg0oXYzAQoBA:GBuG_TLo6SL2OYFxp7Zly04HPNzmAF7Fu2E8E9SnwQDoxq9rf7VThSPtTmnBSAl5UVRRPkMsRtzzHZ87qtj6l-PvF0K7PrDu7-yS_xiFTgAl9sEfXAIHJVzZLoksGRgpoBtpBUg9vVaJsPns0kWFKJgq8M-Mk9agrSk7sb2VUeQ"; 
 
    class TTestUpdater: public TAsyncUpdaterBase { 
    public: 
        using TAsyncUpdaterBase::SetBbEnv; 
        using TAsyncUpdaterBase::SetUserContext; 
 
        TClientStatus GetStatus() const override { 
            return TClientStatus(); 
        } 
    }; 
 
    Y_UNIT_TEST(User) { 
        TTestUpdater u; 
 
        UNIT_ASSERT(!u.GetCachedUserContext()); 
 
        u.SetUserContext(NUnittest::TVMKNIFE_PUBLIC_KEYS); 
        UNIT_ASSERT(!u.GetCachedUserContext()); 
 
        UNIT_ASSERT_NO_EXCEPTION(u.SetBbEnv(EBlackboxEnv::Prod)); 
        UNIT_ASSERT(u.GetCachedUserContext()); 
        UNIT_ASSERT(u.GetCachedUserContext()->Check(PROD_TICKET)); 
        UNIT_ASSERT_NO_EXCEPTION(u.GetCachedUserContext(EBlackboxEnv::ProdYateam)); 
        UNIT_ASSERT(u.GetCachedUserContext(EBlackboxEnv::ProdYateam)->Check(PROD_YATEAM_TICKET)); 
 
        UNIT_ASSERT_EXCEPTION_CONTAINS(u.SetBbEnv(EBlackboxEnv::Prod, EBlackboxEnv::Test), 
                                       TBrokenTvmClientSettings, 
                                       "Overriding of BlackboxEnv is illegal: Prod -> Test"); 
        UNIT_ASSERT_EXCEPTION_CONTAINS(u.GetCachedUserContext(EBlackboxEnv::Test), 
                                       TBrokenTvmClientSettings, 
                                       "Overriding of BlackboxEnv is illegal: Prod -> Test"); 
 
        UNIT_ASSERT(u.GetCachedUserContext()); 
        UNIT_ASSERT(u.GetCachedUserContext()->Check(PROD_TICKET)); 
    } 
 
    class DummyUpdater: public TAsyncUpdaterBase { 
    public: 
        TClientStatus GetStatus() const override { 
            return TClientStatus(); 
        } 
 
        using TAsyncUpdaterBase::SetServiceContext; 
        using TAsyncUpdaterBase::SetServiceTickets; 
        using TAsyncUpdaterBase::SetUserContext; 
    }; 
 
    Y_UNIT_TEST(Cache) { 
        DummyUpdater d; 
 
        UNIT_ASSERT(!d.GetCachedServiceTickets()); 
        TServiceTicketsPtr st = MakeIntrusiveConst<TServiceTickets>(TServiceTickets::TMapIdStr(), 
                                                                    TServiceTickets::TMapIdStr(), 
                                                                    TServiceTickets::TMapAliasId()); 
        d.SetServiceTickets(st); 
        UNIT_ASSERT_EQUAL(st.Get(), d.GetCachedServiceTickets().Get()); 
 
        UNIT_ASSERT(!d.GetCachedServiceContext()); 
        TServiceContextPtr sc = MakeIntrusiveConst<TServiceContext>(TServiceContext::SigningFactory("kjndfadfndsfafdasd")); 
        d.SetServiceContext(sc); 
        UNIT_ASSERT_EQUAL(sc.Get(), d.GetCachedServiceContext().Get()); 
 
        UNIT_ASSERT(!d.GetCachedUserContext()); 
        d.SetUserContext(NUnittest::TVMKNIFE_PUBLIC_KEYS); 
    } 
 
    Y_UNIT_TEST(ServiceTickets_Aliases) { 
        using TId = TServiceTickets::TMapIdStr; 
        using TUnfetchedId = TServiceTickets::TIdSet;
        using TStr = TServiceTickets::TMapAliasStr; 
        using TUnfetchedAlias = TServiceTickets::TAliasSet;
        using TAls = TServiceTickets::TMapAliasId; 
        TServiceTickets t(TId{}, TId{}, TAls{}); 
 
        UNIT_ASSERT_NO_EXCEPTION(t = TServiceTickets(TId({{1, "t1"}, {2, "t2"}}), 
                                                     TId({{3, "e1"}}), 
                                                     TAls())); 
        UNIT_ASSERT_EQUAL(TId({{1, "t1"}, {2, "t2"}}), t.TicketsById); 
        UNIT_ASSERT_EQUAL(TId({{3, "e1"}}), t.ErrorsById); 
        UNIT_ASSERT_EQUAL(TStr(), t.TicketsByAlias); 
        UNIT_ASSERT_EQUAL(TStr(), t.ErrorsByAlias); 
 
        UNIT_ASSERT_NO_EXCEPTION(t = TServiceTickets(TId({{1, "t1"}, {2, "t2"}}), 
                                                     TId({{3, "e1"}}), 
                                                     TAls({{"1", 1}, {"2", 2}, {"3", 3}}))); 
        UNIT_ASSERT_EQUAL(TId({{1, "t1"}, {2, "t2"}}), t.TicketsById); 
        UNIT_ASSERT_EQUAL(TId({{3, "e1"}}), t.ErrorsById); 
        UNIT_ASSERT_EQUAL(TUnfetchedId(), t.UnfetchedIds); 
        UNIT_ASSERT_EQUAL(TStr({{"1", "t1"}, {"2", "t2"}}), t.TicketsByAlias); 
        UNIT_ASSERT_EQUAL(TStr({{"3", "e1"}}), t.ErrorsByAlias); 
        UNIT_ASSERT_EQUAL(TUnfetchedAlias({}), t.UnfetchedAliases); 
    }
 
    Y_UNIT_TEST(ServiceTickets_UnfetchedIds) {
        using TId = TServiceTickets::TMapIdStr;
        using TUnfetchedId = TServiceTickets::TIdSet;
        using TStr = TServiceTickets::TMapAliasStr;
        using TUnfetchedAlias = TServiceTickets::TAliasSet;
        using TAls = TServiceTickets::TMapAliasId;
        TServiceTickets t(TId({{1, "t1"}, {2, "t2"}}), 
                          TId(), 
                          TAls({{"1", 1}, {"2", 2}, {"3", 3}})); 
 
        UNIT_ASSERT_EQUAL(TId({{1, "t1"}, {2, "t2"}}), t.TicketsById); 
        UNIT_ASSERT_EQUAL(TId({}), t.ErrorsById); 
        UNIT_ASSERT_EQUAL(TUnfetchedId({3}), t.UnfetchedIds); 
        UNIT_ASSERT_EQUAL(TUnfetchedAlias({{"3"}}), t.UnfetchedAliases); 
        UNIT_ASSERT_EQUAL(TStr({{"1", "t1"}, {"2", "t2"}}), t.TicketsByAlias); 
        UNIT_ASSERT_EQUAL(TStr(), t.ErrorsByAlias); 
    } 
 
    Y_UNIT_TEST(ServiceTickets_InvalidationTime) { 
        using TId = TServiceTickets::TMapIdStr; 
        using TAls = TServiceTickets::TMapAliasId; 
 
        TServiceTickets t(TId{}, TId{}, TAls{}); 
        UNIT_ASSERT_VALUES_EQUAL(TInstant(), t.InvalidationTime); 
 
        UNIT_ASSERT_NO_EXCEPTION(t = TServiceTickets(TId({{1, SRV_TICKET}}), 
                                                     TId(), 
                                                     TAls())); 
        UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(std::numeric_limits<time_t>::max()), t.InvalidationTime); 
 
        UNIT_ASSERT_NO_EXCEPTION(t = TServiceTickets(TId({ 
                                                         {1, SRV_TICKET}, 
                                                         {2, "serv"}, 
                                                     }), 
                                                     TId(), 
                                                     TAls())); 
        UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(std::numeric_limits<time_t>::max()), t.InvalidationTime); 
 
        UNIT_ASSERT_NO_EXCEPTION(t = TServiceTickets(TId({ 
                                                         {2, "serv"}, 
                                                     }), 
                                                     TId(), 
                                                     TAls())); 
        UNIT_ASSERT_VALUES_EQUAL(TInstant(), t.InvalidationTime); 
 
        UNIT_ASSERT_NO_EXCEPTION(t = TServiceTickets(TId({ 
                                                         {1, SRV_TICKET}, 
                                                         {2, "serv"}, 
                                                         {3, "3:serv:CBAQeyIECAMQAw:TiZjG2Ut9j-9n0zcqxGW8xiYmnFa-i10-dbA0FKIInKzeDuueovWVEBcgbQHndblzRCxoIBMgbotOf7ALk2xoSBnRbOKomAIEtiTBL77GByL5O8K_HUGNYb-ygqnmZlIuLalgeRQAdsKstgUwQzufnOQyekipmamwo7EVQhr8Ug"}, 
                                                     }), 
                                                     TId(), 
                                                     TAls())); 
        UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(123), t.InvalidationTime); 
    } 
} 
