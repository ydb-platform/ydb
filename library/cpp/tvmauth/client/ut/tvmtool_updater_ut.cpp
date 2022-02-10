#include "common.h" 
 
#include <library/cpp/tvmauth/client/facade.h> 
#include <library/cpp/tvmauth/client/misc/tool/threaded_updater.h> 
 
#include <library/cpp/http/simple/http_client.h> 
#include <library/cpp/testing/unittest/registar.h>
 
#include <util/system/env.h> 
 
using namespace NTvmAuth; 
using namespace NTvmAuth::NTvmTool; 
 
Y_UNIT_TEST_SUITE(ToolUpdater) { 
    static const TString SRV_TICKET = "3:serv:CBAQ__________9_IgYIexCUkQY:GioCM49Ob6_f80y6FY0XBVN4hLXuMlFeyMvIMiDuQnZkbkLpRpQOuQo5YjWoBjM0Vf-XqOm8B7xtrvxSYHDD7Q4OatN2l-Iwg7i71lE3scUeD36x47st3nd0OThvtjrFx_D8mw_c0GT5KcniZlqq1SjhLyAk1b_zJsx8viRAhCU"; 
    static const TString SRV_TICKET_DST_100503 = "3:serv:CBAQ__________9_IggIwMQHEJeRBg:Kj7VApP6D91UJ8pKpeaE3vYaNTBBJcdYpJLbF9w2-Mb-75s_SmMKkPqqA2rMS358uFfoYpv9YZxq0tIaUj5HPQ1WaQ1yiVuPZ_oi3pJRdr006eRyihM8PUfl6m9ioCFftfOcAg9oN5BGeHTNhn7VWuj3yMg7feaMB0zAUpyaPG0"; 
    static const TString TEST_TICKET = "3:user:CA0Q__________9_Gg4KAgh7EHsg0oXYzAQoAQ:FSADps3wNGm92Vyb1E9IVq5M6ZygdGdt1vafWWEhfDDeCLoVA-sJesxMl2pGW4OxJ8J1r_MfpG3ZoBk8rLVMHUFrPa6HheTbeXFAWl8quEniauXvKQe4VyrpA1SPgtRoFqi5upSDIJzEAe1YRJjq1EClQ_slMt8R0kA_JjKUX54"; 
    static const TString PROD_YATEAM_TICKET = "3:user:CAwQ__________9_Gg4KAgh7EHsg0oXYzAQoAg:G2wloFRSi8--RLb2GDSro_sKXPF2JSdL5CVOuOHgUcRvLm-3OxIPn0NUqbJ9DWDmhPplOqEiblIbLK85My1VMJ2aG5SLbRNKEtwfmxLvkwNpl_gUEwWPJm9_8Khslfj71P3hccxtEEqM9bJSMwHueVAY-a9HSzFo-uMFMeSgQ-k"; 
 
    class TMetaInfoProxy: public TMetaInfo { 
    public: 
        using TMetaInfo::ApplySettings; 
        using TMetaInfo::BbEnvFromString; 
        using TMetaInfo::Config_; 
        using TMetaInfo::Fetch; 
        using TMetaInfo::ParseMetaString; 
        using TMetaInfo::TMetaInfo; 
    }; 
 
    Y_UNIT_TEST(Settings) { 
        NTvmTool::TClientSettings s("foo"); 
        UNIT_ASSERT_EXCEPTION_CONTAINS(s.SetAuthToken("\n "), 
                                       TBrokenTvmClientSettings, 
                                       "Auth token cannot be empty"); 
        UNIT_ASSERT_EXCEPTION_CONTAINS(s.GetAuthToken(), 
                                       TBrokenTvmClientSettings, 
                                       "Auth token cannot be empty. Env 'TVMTOOL_LOCAL_AUTHTOKEN' and 'QLOUD_TVM_TOKEN' are empty."); 
 
        UNIT_ASSERT_NO_EXCEPTION(s.SetAuthToken(AUTH_TOKEN + "\n")); 
        UNIT_ASSERT_VALUES_EQUAL(AUTH_TOKEN, s.GetAuthToken()); 
 
        UNIT_ASSERT_VALUES_EQUAL("localhost", s.GetHostname()); 
        UNIT_ASSERT_EXCEPTION_CONTAINS(s.SetHostname(""), 
                                       TBrokenTvmClientSettings, 
                                       "Hostname cannot be empty"); 
 
        UNIT_ASSERT_NO_EXCEPTION(s.SetHostname("qwe")); 
        UNIT_ASSERT_VALUES_EQUAL("qwe", s.GetHostname()); 
    } 
 
    Y_UNIT_TEST(SettingsCtor) { 
        UNIT_ASSERT_EXCEPTION_CONTAINS(NTvmTool::TClientSettings(""), 
                                       TBrokenTvmClientSettings, 
                                       "Alias for your TVM client cannot be empty"); 
        { 
            NTvmTool::TClientSettings s("self"); 
            UNIT_ASSERT_EXCEPTION_CONTAINS(s.GetAuthToken(), 
                                           TBrokenTvmClientSettings, 
                                           "Auth token cannot be empty. " 
                                           "Env 'TVMTOOL_LOCAL_AUTHTOKEN' and 'QLOUD_TVM_TOKEN' are empty."); 
        } 
 
        struct TEnvs { 
            TEnvs(const std::map<TString, TString>& Env) { 
                for (const auto& [key, value] : Env) { 
                    Prev[key] = GetEnv(key); 
                    SetEnv(key, value); 
                } 
            } 
 
            ~TEnvs() { 
                for (const auto& [key, value] : Prev) { 
                    SetEnv(key, value); 
                } 
            } 
 
            std::map<TString, TString> Prev; 
        }; 
 
        struct TCase { 
            std::map<TString, TString> Env; 
            TString AuthToken; 
            ui16 Port = 0; 
        }; 
 
        std::vector<TCase> cases = { 
            { 
                { 
                    {"TVMTOOL_LOCAL_AUTHTOKEN", "qwerty"}, 
                }, 
                "qwerty", 
                1, 
            }, 
            { 
                { 
                    {"TVMTOOL_LOCAL_AUTHTOKEN", "qwerty"}, 
                    {"QLOUD_TVM_TOKEN", "zxcvbn"}, 
                }, 
                "qwerty", 
                1, 
            }, 
            { 
                { 
                    {"QLOUD_TVM_TOKEN", "zxcvbn"}, 
                }, 
                "zxcvbn", 
                1, 
            }, 
            { 
                { 
                    {"TVMTOOL_LOCAL_AUTHTOKEN", "qwerty"}, 
                    {"DEPLOY_TVM_TOOL_URL", "32272"}, 
                }, 
                "qwerty", 
                1, 
            }, 
            { 
                { 
                    {"TVMTOOL_LOCAL_AUTHTOKEN", "qwerty"}, 
                    {"DEPLOY_TVM_TOOL_URL", "localhost:32272"}, 
                }, 
                "qwerty", 
                32272, 
            }, 
            { 
                { 
                    {"TVMTOOL_LOCAL_AUTHTOKEN", "qwerty"}, 
                    {"DEPLOY_TVM_TOOL_URL", "http://localhost:32272"}, 
                }, 
                "qwerty", 
                32272, 
            }, 
        }; 
 
        for (const TCase& c : cases) { 
            TEnvs envs(c.Env); 
 
            NTvmTool::TClientSettings s("self"); 
            UNIT_ASSERT_VALUES_EQUAL(c.AuthToken, s.GetAuthToken()); 
            UNIT_ASSERT_VALUES_EQUAL(c.Port, s.GetPort()); 
        } 
    } 
 
    Y_UNIT_TEST(Meta_Fetch) { 
        TPortManager pm; 
        ui16 port = pm.GetPort(80); 
        NMock::TMockServer server(port, []() { return new TTvmTool; }); 
        TKeepAliveHttpClient client("localhost", port); 
 
        TMetaInfoProxy m(nullptr); 
        NTvmTool::TClientSettings settings("me"); 
        settings.SetAuthToken(AUTH_TOKEN); 
        m.ApplySettings(settings); 
 
        UNIT_ASSERT_VALUES_EQUAL(META, m.Fetch(client)); 
 
        settings.SetAuthToken("qwerty"); 
        m.ApplySettings(settings); 
        UNIT_ASSERT_EXCEPTION_CONTAINS(m.Fetch(client), 
                                       TNonRetriableException, 
                                       "Failed to fetch meta from tvmtool: localhost:"); 
 
        settings.SetAuthToken(AUTH_TOKEN); 
        m.ApplySettings(settings); 
        { 
            TKeepAliveHttpClient client("localhost", 0); 
            UNIT_ASSERT_EXCEPTION_CONTAINS(m.Fetch(client), 
                                           TRetriableException, 
                                           "Failed to fetch meta data from tvmtool: "); 
        } 
 
        server.SetGenerator([]() { 
            auto p = new TTvmTool; 
            p->Code = HTTP_NOT_FOUND; 
            return p; }); 
        UNIT_ASSERT_EXCEPTION_CONTAINS(m.Fetch(client), 
                                       TNonRetriableException, 
                                       "Library does not support so old tvmtool. You need tvmtool>=1.1.0"); 
        server.SetGenerator([]() { 
            auto p = new TTvmTool; 
            p->Code = HTTP_INTERNAL_SERVER_ERROR; 
            return p; }); 
        UNIT_ASSERT_EXCEPTION_CONTAINS(m.Fetch(client), 
                                       TRetriableException, 
                                       "Failed to fetch meta from tvmtool: localhost:"); 
    } 
 
    Y_UNIT_TEST(Meta_ParseMetaString_me) { 
        TMetaInfo::TConfigPtr c; 
        UNIT_ASSERT(c = TMetaInfoProxy::ParseMetaString(META, "me")); 
        UNIT_ASSERT_VALUES_EQUAL(100500, c->SelfTvmId); 
        UNIT_ASSERT_EQUAL(EBlackboxEnv::ProdYateam, c->BbEnv); 
        UNIT_ASSERT_EQUAL(TMetaInfo::TDstAliases({{"bbox", 242}, {"pass_likers", 11}}), c->DstAliases); 
    } 
 
    Y_UNIT_TEST(Meta_ParseMetaString_pc) { 
        TMetaInfo::TConfigPtr c; 
        UNIT_ASSERT(c = TMetaInfoProxy::ParseMetaString(META, "push-client")); 
        UNIT_ASSERT_VALUES_EQUAL(100501, c->SelfTvmId); 
        UNIT_ASSERT_EQUAL(EBlackboxEnv::ProdYateam, c->BbEnv); 
        UNIT_ASSERT_EQUAL(TMetaInfo::TDstAliases({{"pass_likers", 100502}}), c->DstAliases); 
    } 
 
    Y_UNIT_TEST(Meta_ParseMetaString_se) { 
        TMetaInfo::TConfigPtr c; 
        UNIT_ASSERT(c = TMetaInfoProxy::ParseMetaString(META, "something_else")); 
        UNIT_ASSERT_VALUES_EQUAL(100503, c->SelfTvmId); 
        UNIT_ASSERT_EQUAL(EBlackboxEnv::ProdYateam, c->BbEnv); 
        UNIT_ASSERT(c->DstAliases.empty()); 
    } 
 
    Y_UNIT_TEST(Meta_ParseMetaString_errors) { 
        TMetaInfoProxy m(nullptr); 
        UNIT_ASSERT(!m.ParseMetaString(META, "ololo")); 
 
        TString meta = "}"; 
        UNIT_ASSERT_EXCEPTION_CONTAINS(m.ParseMetaString(meta, "qqq"), yexception, meta); 
        meta = "{}"; 
        UNIT_ASSERT_EXCEPTION_CONTAINS(m.ParseMetaString(meta, "qqq"), yexception, meta); 
        meta = R"({"tenants" : {}})"; 
        UNIT_ASSERT_EXCEPTION_CONTAINS(m.ParseMetaString(meta, "qqq"), yexception, meta); 
        meta = R"({"tenants" : [{"self":{}}]})"; 
        UNIT_ASSERT_EXCEPTION_CONTAINS(m.ParseMetaString(meta, "qqq"), yexception, meta); 
    } 
 
    Y_UNIT_TEST(Meta_BbEnvFromString) { 
        UNIT_ASSERT_VALUES_EQUAL(EBlackboxEnv::Prod, TMetaInfoProxy::BbEnvFromString("Prod", META)); 
        UNIT_ASSERT_VALUES_EQUAL(EBlackboxEnv::Test, TMetaInfoProxy::BbEnvFromString("Test", META)); 
        UNIT_ASSERT_VALUES_EQUAL(EBlackboxEnv::ProdYateam, TMetaInfoProxy::BbEnvFromString("ProdYaTeam", META)); 
        UNIT_ASSERT_VALUES_EQUAL(EBlackboxEnv::TestYateam, TMetaInfoProxy::BbEnvFromString("TestYaTeam", META)); 
        UNIT_ASSERT_VALUES_EQUAL(EBlackboxEnv::Stress, TMetaInfoProxy::BbEnvFromString("Stress", META)); 
        UNIT_ASSERT_EXCEPTION_CONTAINS(TMetaInfoProxy::BbEnvFromString("foo", META), 
                                       yexception, 
                                       "'bb_env'=='foo'"); 
    } 
 
    Y_UNIT_TEST(Meta_ApplySettings) { 
        NTvmTool::TClientSettings s("foo"); 
        s.SetAuthToken(AUTH_TOKEN); 
 
        TMetaInfoProxy m(nullptr); 
        m.ApplySettings(s); 
 
        UNIT_ASSERT_VALUES_EQUAL( 
            TKeepAliveHttpClient::THeaders({{"Authorization", AUTH_TOKEN}}), 
            m.GetAuthHeader()); 
    } 
 
    Y_UNIT_TEST(Meta_Init) { 
        TPortManager pm; 
        ui16 port = pm.GetPort(80); 
        NMock::TMockServer server(port, []() { return new TTvmTool; }); 
        TKeepAliveHttpClient client("localhost", port); 
 
        NTvmTool::TClientSettings s("me"); 
        s.SetAuthToken(AUTH_TOKEN); 
        s.SetPort(port); 
        auto l = MakeIntrusive<TLogger>(); 
        TMetaInfo m(l); 
        UNIT_ASSERT_NO_EXCEPTION(m.Init(client, s)); 
        UNIT_ASSERT_VALUES_EQUAL(100500, m.GetConfig()->SelfTvmId); 
        UNIT_ASSERT_EQUAL(EBlackboxEnv::ProdYateam, m.GetConfig()->BbEnv); 
        UNIT_ASSERT_EQUAL(TMetaInfo::TDstAliases({{"bbox", 242}, {"pass_likers", 11}}), m.GetConfig()->DstAliases); 
        UNIT_ASSERT_VALUES_EQUAL(TStringBuilder() 
                                     << "7: Meta info fetched from localhost:" << port << "\n" 
                                     << "6: Meta: self_tvm_id=100500, bb_env=ProdYateam, dsts=[(pass_likers:11)(bbox:242)]\n", 
                                 l->Stream.Str()); 
        l->Stream.Clear(); 
        UNIT_ASSERT_VALUES_EQUAL( 
            "/tvm/tickets?src=100500&dsts=11,242", 
            TMetaInfo::GetRequestForTickets(*m.GetConfig())); 
 
        server.SetGenerator([]() { 
            auto p = new TTvmTool; 
            p->Meta = R"({ 
        "bb_env" : "Prod", 
        "tenants" : [{ 
                "self": {"alias" : "me", "client_id": 100500}, 
                "dsts" : [{"alias" : "pass_likers","client_id": 11}] 
            }] 
        })"; 
            return p; }); 
        UNIT_ASSERT(m.TryUpdateConfig(client)); 
        UNIT_ASSERT_VALUES_EQUAL( 
            "6: Meta was updated. Old: (self_tvm_id=100500, bb_env=ProdYateam, dsts=[(pass_likers:11)(bbox:242)]). New: (self_tvm_id=100500, bb_env=Prod, dsts=[(pass_likers:11)])\n", 
            l->Stream.Str()); 
        l->Stream.clear(); 
 
        s = NTvmTool::TClientSettings("foo"); 
        s.SetAuthToken(AUTH_TOKEN); 
        s.SetPort(port); 
        TMetaInfo m2(l); 
        UNIT_ASSERT_EXCEPTION_CONTAINS(m2.Init(client, s), TNonRetriableException, "Alias 'foo' not found in meta info"); 
        UNIT_ASSERT_VALUES_EQUAL(TStringBuilder() 
                                     << "7: Meta info fetched from localhost:" << port << "\n", 
                                 l->Stream.Str()); 
        UNIT_ASSERT_EXCEPTION_CONTAINS(TMetaInfo::GetRequestForTickets({}), 
                                       yexception, 
                                       "DstAliases.empty()"); 
 
        server.SetGenerator([]() { 
            auto p = new TTvmTool; 
            p->Meta = "}"; 
            return p; }); 
        UNIT_ASSERT_EXCEPTION_CONTAINS(m.Init(client, s), 
                                       TNonRetriableException, 
                                       "Malformed json from tvmtool:"); 
    } 
 
    class TNonInitedUpdater: public TThreadedUpdater { 
    public: 
        TNonInitedUpdater(const TString& host, ui16 port, TLoggerPtr logger) 
            : TThreadedUpdater(host, port, TDuration::Seconds(5), TDuration::Seconds(30), logger)
        { 
        } 
 
        using TThreadedUpdater::ArePublicKeysOk; 
        using TThreadedUpdater::AreServiceTicketsOk; 
        using TThreadedUpdater::FetchPublicKeys; 
        using TThreadedUpdater::FetchServiceTickets; 
        using TThreadedUpdater::GetBirthTimeFromResponse; 
        using TThreadedUpdater::Init; 
        using TThreadedUpdater::IsTimeToUpdatePublicKeys; 
        using TThreadedUpdater::IsTimeToUpdateServiceTickets; 
        using TThreadedUpdater::LastVisitForConfig_; 
        using TThreadedUpdater::MetaInfo_; 
        using TThreadedUpdater::ParseFetchTicketsResponse; 
        using TThreadedUpdater::SetBbEnv; 
        using TThreadedUpdater::SetServiceContext; 
        using TThreadedUpdater::SetServiceTickets; 
        using TThreadedUpdater::SetUpdateTimeOfPublicKeys; 
        using TThreadedUpdater::SetUpdateTimeOfServiceTickets; 
        using TThreadedUpdater::SetUserContext; 
        using TThreadedUpdater::TPairTicketsErrors; 
        using TThreadedUpdater::UpdateKeys; 
        using TThreadedUpdater::UpdateServiceTickets; 
    }; 
 
    Y_UNIT_TEST(GetBirthTimeFromResponse) { 
        THttpHeaders h; 
        UNIT_ASSERT_EXCEPTION_CONTAINS(TNonInitedUpdater::GetBirthTimeFromResponse(h, "ololo"), 
                                       yexception, 
                                       "Failed to fetch bithtime of ololo from tvmtool"); 
 
        h.AddHeader(THttpInputHeader("X-Ya-Tvmtool-Data-Birthtime: qwe")); 
        UNIT_ASSERT_EXCEPTION_CONTAINS(TNonInitedUpdater::GetBirthTimeFromResponse(h, "ololo"), 
                                       yexception, 
                                       "Bithtime of ololo from tvmtool must be unixtime. Got: qwe"); 
 
        h.AddOrReplaceHeader(THttpInputHeader("X-Ya-Tvmtool-Data-Birthtime: 123")); 
        UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(123), TNonInitedUpdater::GetBirthTimeFromResponse(h, "ololo")); 
    } 
 
    Y_UNIT_TEST(Fetch) { 
        TPortManager pm; 
        ui16 port = pm.GetPort(80); 
        NMock::TMockServer server(port, []() { return new TTvmTool; }); 
        TKeepAliveHttpClient client("localhost", port); 
 
        auto l = MakeIntrusive<TLogger>(); 
        TNonInitedUpdater u("localhost", port, l); 
        NTvmTool::TClientSettings s("me"); 
        s.SetAuthToken(AUTH_TOKEN); 
        s.SetPort(port); 
        u.MetaInfo_.Init(client, s); 
        auto p = u.FetchPublicKeys(); 
        UNIT_ASSERT_STRINGS_EQUAL(NUnittest::TVMKNIFE_PUBLIC_KEYS, p.first); 
        UNIT_ASSERT_VALUES_EQUAL(BIRTHTIME, p.second); 
 
        auto p2 = u.FetchServiceTickets(*u.MetaInfo_.GetConfig()); 
        UNIT_ASSERT_STRINGS_EQUAL(TICKETS_ME, p2.first); 
        UNIT_ASSERT_VALUES_EQUAL(BIRTHTIME, p2.second); 
 
        UNIT_ASSERT_VALUES_EQUAL( 
            TStringBuilder() 
                << "7: Meta info fetched from localhost:" << port << "\n" 
                << "6: Meta: self_tvm_id=100500, bb_env=ProdYateam, dsts=[(pass_likers:11)(bbox:242)]\n", 
            l->Stream.Str()); 
    } 
 
    Y_UNIT_TEST(ParseFetchTicketsResponse) { 
        auto l = MakeIntrusive<TLogger>(); 
        TNonInitedUpdater u("", 0, l); 
 
        UNIT_ASSERT_EXCEPTION_CONTAINS(u.ParseFetchTicketsResponse("}", {}), 
                                       yexception, 
                                       "Invalid json from tvmtool: }"); 
 
        auto t = u.ParseFetchTicketsResponse(TICKETS_ME, {{"pass_likers", 11}, {"se", 2}}); 
        auto expected = TNonInitedUpdater::TPairTicketsErrors{ 
            {{11, "3:serv:CBAQ__________9_IgYIlJEGEAs:T-apeMNWFc_vHPQ3iLaZv9NjG-hf5-i23O4AhRu1M68ryN3FU5qvyqTSSiPbtJdFP6EE41QQBzEs59dHn9DRkqQNwwKf1is00Oewwj2XKO0uHukuzd9XxZnro7MfjPswsjWufxX28rmJtlfSXwAtyKt8TI5yKJnMeBPQ0m5R3k8"}}, 
            { 
                {2, "Missing tvm_id in response, should never happend: se"}, 
            }, 
        }; 
        UNIT_ASSERT_VALUES_EQUAL(expected, t); 
 
        UNIT_ASSERT_VALUES_EQUAL( 
            TStringBuilder() 
                << "3: Failed to get ServiceTicket for se (2): Missing tvm_id in response, should never happend: se\n", 
            l->Stream.Str()); 
    } 
 
    Y_UNIT_TEST(Update) { 
        TPortManager pm; 
        ui16 port = pm.GetPort(80); 
        NMock::TMockServer server(port, []() { return new TTvmTool; }); 
        TKeepAliveHttpClient client("localhost", port); 
 
        auto l = MakeIntrusive<TLogger>(); 
        TNonInitedUpdater u("localhost", port, l); 
        NTvmTool::TClientSettings s("me"); 
        s.SetAuthToken(AUTH_TOKEN); 
        s.SetPort(port); 
        u.MetaInfo_.Init(client, s); 
 
        using TTickets = TServiceTickets::TMapAliasStr; 
        UNIT_ASSERT(!u.GetCachedServiceTickets()); 
        UNIT_ASSERT_VALUES_EQUAL(BIRTHTIME, u.UpdateServiceTickets(*u.MetaInfo_.GetConfig())); 
        UNIT_ASSERT(u.GetCachedServiceTickets()); 
        UNIT_ASSERT_VALUES_EQUAL(TInstant(), u.GetUpdateTimeOfServiceTickets()); 
        UNIT_ASSERT_EQUAL( 
            TTickets({ 
                {"bbox", "3:serv:CBAQ__________9_IgcIlJEGEPIB:N7luw0_rVmBosTTI130jwDbQd0-cMmqJeEl0ma4ZlIo_mHXjBzpOuMQ3A9YagbmOBOt8TZ_gzGvVSegWZkEeB24gM22acw0w-RcHaQKrzSOA5Zq8WLNIC8QUa4_WGTlAsb7R7eC4KTAGgouIquNAgMBdTuGOuZHnMLvZyLnOMKc"}, 
                {"pass_likers", "3:serv:CBAQ__________9_IgYIlJEGEAs:T-apeMNWFc_vHPQ3iLaZv9NjG-hf5-i23O4AhRu1M68ryN3FU5qvyqTSSiPbtJdFP6EE41QQBzEs59dHn9DRkqQNwwKf1is00Oewwj2XKO0uHukuzd9XxZnro7MfjPswsjWufxX28rmJtlfSXwAtyKt8TI5yKJnMeBPQ0m5R3k8"}, 
            }), 
            u.GetCachedServiceTickets()->TicketsByAlias); 
        UNIT_ASSERT_VALUES_EQUAL( 
            TStringBuilder() 
                << "7: Meta info fetched from localhost:" << port << "\n" 
                << "6: Meta: self_tvm_id=100500, bb_env=ProdYateam, dsts=[(pass_likers:11)(bbox:242)]\n", 
            l->Stream.Str()); 
        l->Stream.Clear(); 
 
        UNIT_ASSERT(!u.GetCachedServiceContext()); 
        UNIT_ASSERT(!u.GetCachedUserContext()); 
        UNIT_ASSERT_VALUES_EQUAL(BIRTHTIME, u.UpdateKeys(*u.MetaInfo_.GetConfig())); 
        UNIT_ASSERT(u.GetCachedServiceContext()); 
        UNIT_ASSERT(!u.GetCachedUserContext()); 
        u.SetBbEnv(EBlackboxEnv::Test); 
        UNIT_ASSERT(u.GetCachedUserContext()); 
        UNIT_ASSERT_VALUES_EQUAL("", l->Stream.Str()); 
        l->Stream.Clear(); 
 
        { 
            TAsyncUpdaterPtr u = TThreadedUpdater::Create(s, l); 
            UNIT_ASSERT(u->GetCachedServiceTickets()); 
            UNIT_ASSERT(u->GetCachedServiceContext()); 
            UNIT_ASSERT(u->GetCachedUserContext()); 
            UNIT_ASSERT_VALUES_EQUAL(TClientStatus::Ok, u->GetStatus()); 
 
            NTvmAuth::TTvmClient c(u); 
            UNIT_ASSERT(c.CheckServiceTicket(SRV_TICKET)); 
            UNIT_ASSERT(!c.CheckServiceTicket(SRV_TICKET_DST_100503)); 
            UNIT_ASSERT(c.CheckUserTicket(PROD_YATEAM_TICKET)); 
            UNIT_ASSERT_VALUES_EQUAL("3:serv:CBAQ__________9_IgYIlJEGEAs:T-apeMNWFc_vHPQ3iLaZv9NjG-hf5-i23O4AhRu1M68ryN3FU5qvyqTSSiPbtJdFP6EE41QQBzEs59dHn9DRkqQNwwKf1is00Oewwj2XKO0uHukuzd9XxZnro7MfjPswsjWufxX28rmJtlfSXwAtyKt8TI5yKJnMeBPQ0m5R3k8", c.GetServiceTicketFor("pass_likers")); 
        } 
        UNIT_ASSERT_VALUES_EQUAL( 
            TStringBuilder() 
                << "7: Meta info fetched from localhost:" << port << "\n" 
                << "6: Meta: self_tvm_id=100500, bb_env=ProdYateam, dsts=[(pass_likers:11)(bbox:242)]\n" 
                << "7: Tickets fetched from tvmtool: 2425-09-17T11:04:00.000000Z\n" 
                << "7: Public keys fetched from tvmtool: 2425-09-17T11:04:00.000000Z\n" 
                << "7: Thread-worker started\n" 
                << "7: Thread-worker stopped\n", 
            l->Stream.Str()); 
        l->Stream.Clear(); 
 
        { 
            NTvmTool::TClientSettings s("something_else"); 
            s.SetAuthToken(AUTH_TOKEN); 
            s.SetPort(port); 
 
            TAsyncUpdaterPtr u = TThreadedUpdater::Create(s, l); 
            UNIT_ASSERT(!u->GetCachedServiceTickets()); 
            UNIT_ASSERT(u->GetCachedServiceContext()); 
            UNIT_ASSERT(u->GetCachedUserContext()); 
            UNIT_ASSERT_VALUES_EQUAL(TClientStatus::Ok, u->GetStatus()); 
 
            NTvmAuth::TTvmClient c(u); 
            UNIT_ASSERT(!c.CheckServiceTicket(SRV_TICKET)); 
            UNIT_ASSERT(c.CheckServiceTicket(SRV_TICKET_DST_100503)); 
            UNIT_ASSERT(c.CheckUserTicket(PROD_YATEAM_TICKET)); 
            UNIT_ASSERT_EXCEPTION_CONTAINS(c.GetServiceTicketFor("pass_likers"), 
                                           TBrokenTvmClientSettings, 
                                           "Need to enable ServiceTickets fetching"); 
        } 
        UNIT_ASSERT_VALUES_EQUAL( 
            TStringBuilder() 
                << "7: Meta info fetched from localhost:" << port << "\n" 
                << "6: Meta: self_tvm_id=100503, bb_env=ProdYateam, dsts=[]\n" 
                << "7: Public keys fetched from tvmtool: 2425-09-17T11:04:00.000000Z\n" 
                << "7: Thread-worker started\n" 
                << "7: Thread-worker stopped\n", 
            l->Stream.Str()); 
        l->Stream.Clear(); 
    } 
 
    Y_UNIT_TEST(IsOk) { 
        TNonInitedUpdater u("", 0, TDevNullLogger::IAmBrave()); 
        using TTickets = TServiceTickets::TMapIdStr; 
 
        UNIT_ASSERT(u.AreServiceTicketsOk(0)); 
        UNIT_ASSERT(!u.AreServiceTicketsOk(2)); 
        u.SetServiceTickets(MakeIntrusiveConst<TServiceTickets>(TTickets(), 
                                                                TTickets(), 
                                                                TServiceTickets::TMapAliasId())); 
        UNIT_ASSERT(u.AreServiceTicketsOk(0)); 
        UNIT_ASSERT(!u.AreServiceTicketsOk(2)); 
        u.SetServiceTickets(MakeIntrusiveConst<TServiceTickets>( 
            TTickets({ 
                {1, "mega_ticket"}, 
                {2, "mega_ticket2"}, 
            }), 
            TTickets({ 
                {3, "mega_error3"}, 
            }), 
            TServiceTickets::TMapAliasId())); 
        UNIT_ASSERT(u.AreServiceTicketsOk(0)); 
        UNIT_ASSERT(!u.AreServiceTicketsOk(2)); 
 
        u.SetServiceTickets(MakeIntrusiveConst<TServiceTickets>( 
            TTickets({ 
                {1, "mega_ticket"}, 
                {2, "mega_ticket2"}, 
            }), 
            TTickets({ 
                {3, "mega_error3"}, 
            }), 
            TServiceTickets::TMapAliasId({ 
                {"mega_ticket", 1}, 
                {"mega_ticket2", 2}, 
                {"mega_ticket3", 3}, 
            }))); 
        UNIT_ASSERT(u.AreServiceTicketsOk(2)); 
 
        UNIT_ASSERT(!u.ArePublicKeysOk()); 
        u.SetServiceContext(MakeIntrusiveConst<TServiceContext>( 
            TServiceContext::CheckingFactory(12, NUnittest::TVMKNIFE_PUBLIC_KEYS))); 
        UNIT_ASSERT(!u.ArePublicKeysOk()); 
        u.SetUserContext(NUnittest::TVMKNIFE_PUBLIC_KEYS); 
        UNIT_ASSERT(!u.ArePublicKeysOk()); 
        u.SetBbEnv(EBlackboxEnv::Test); 
        UNIT_ASSERT(u.ArePublicKeysOk()); 
    } 
 
    Y_UNIT_TEST(IsTimeToUpdate) { 
        TNonInitedUpdater u("", 0, TDevNullLogger::IAmBrave()); 
 
        UNIT_ASSERT(!u.IsTimeToUpdatePublicKeys(TInstant::Now() - TDuration::Seconds(597))); 
        UNIT_ASSERT(u.IsTimeToUpdatePublicKeys(TInstant::Now() - TDuration::Seconds(603))); 
 
        TMetaInfo::TConfig cfg; 
        UNIT_ASSERT(!u.IsTimeToUpdateServiceTickets(cfg, TInstant::Now() - TDuration::Seconds(597))); 
        UNIT_ASSERT(!u.IsTimeToUpdateServiceTickets(cfg, TInstant::Now() - TDuration::Seconds(603))); 
 
        cfg.DstAliases = {{"q", 1}}; 
        UNIT_ASSERT(!u.IsTimeToUpdateServiceTickets(cfg, TInstant::Now() - TDuration::Seconds(597))); 
        UNIT_ASSERT(u.IsTimeToUpdateServiceTickets(cfg, TInstant::Now() - TDuration::Seconds(603))); 
    } 
 
    Y_UNIT_TEST(InitWithOldData) { 
        TPortManager pm; 
        ui16 port = pm.GetPort(80); 
        NMock::TMockServer server(port, 
                                  []() { 
                                      auto p = new TTvmTool; 
                                      p->Birthtime = TInstant::Seconds(123); 
                                      return p; 
                                  }); 
 
        NTvmTool::TClientSettings s("me"); 
        s.SetAuthToken(AUTH_TOKEN); 
        s.SetPort(port); 
 
        auto l = MakeIntrusive<TLogger>(); 
        UNIT_ASSERT_EXCEPTION_CONTAINS(TThreadedUpdater::Create(s, l), 
                                       TRetriableException, 
                                       "Failed to start TvmClient. You can retry: "); 
        UNIT_ASSERT_VALUES_EQUAL( 
            TStringBuilder() 
                << "7: Meta info fetched from localhost:" << port << "\n" 
                << "6: Meta: self_tvm_id=100500, bb_env=ProdYateam, dsts=[(pass_likers:11)(bbox:242)]\n" 
                << "4: Error while fetching of tickets: Service tickets are too old: 1970-01-01T00:02:03.000000Z\n" 
                << "3: Service tickets have not been refreshed for too long period\n" 
                << "4: Error while fetching of public keys: Public keys are too old: 1970-01-01T00:02:03.000000Z\n" 
                << "3: Public keys have not been refreshed for too long period\n" 
                << "4: Error while fetching of tickets: Service tickets are too old: 1970-01-01T00:02:03.000000Z\n" 
                << "3: Service tickets have not been refreshed for too long period\n" 
                << "4: Error while fetching of public keys: Public keys are too old: 1970-01-01T00:02:03.000000Z\n" 
                << "3: Public keys have not been refreshed for too long period\n" 
                << "4: Error while fetching of tickets: Service tickets are too old: 1970-01-01T00:02:03.000000Z\n" 
                << "3: Service tickets have not been refreshed for too long period\n" 
                << "4: Error while fetching of public keys: Public keys are too old: 1970-01-01T00:02:03.000000Z\n" 
                << "3: Public keys have not been refreshed for too long period\n", 
            l->Stream.Str()); 
    } 
 
    Y_UNIT_TEST(InitWithOldData_onlyKeys) { 
        TPortManager pm; 
        ui16 port = pm.GetPort(80); 
        NMock::TMockServer server(port, 
                                  []() { 
                                      auto p = new TTvmTool; 
                                      p->Birthtime = TInstant::Seconds(123); 
                                      return p; 
                                  }); 
 
        NTvmTool::TClientSettings s("something_else"); 
        s.SetAuthToken(AUTH_TOKEN); 
        s.SetPort(port); 
 
        { 
            s.OverrideBlackboxEnv(EBlackboxEnv::Stress); 
            auto l = MakeIntrusive<TLogger>(); 
            UNIT_ASSERT_EXCEPTION_CONTAINS(TThreadedUpdater::Create(s, l), 
                                           TBrokenTvmClientSettings, 
                                           "Overriding of BlackboxEnv is illegal: ProdYateam -> Stress"); 
        } 
 
        s.OverrideBlackboxEnv(EBlackboxEnv::Prod); 
        auto l = MakeIntrusive<TLogger>(); 
        UNIT_ASSERT_EXCEPTION_CONTAINS(TThreadedUpdater::Create(s, l), 
                                       TRetriableException, 
                                       "Failed to start TvmClient. You can retry: "); 
        UNIT_ASSERT_VALUES_EQUAL( 
            TStringBuilder() 
                << "7: Meta info fetched from localhost:" << port << "\n" 
                << "6: Meta: self_tvm_id=100503, bb_env=ProdYateam, dsts=[]\n" 
                << "6: Meta: override blackbox env: ProdYateam->Prod\n" 
                << "4: Error while fetching of public keys: Public keys are too old: 1970-01-01T00:02:03.000000Z\n" 
                << "3: Public keys have not been refreshed for too long period\n" 
                << "4: Error while fetching of public keys: Public keys are too old: 1970-01-01T00:02:03.000000Z\n" 
                << "3: Public keys have not been refreshed for too long period\n" 
                << "4: Error while fetching of public keys: Public keys are too old: 1970-01-01T00:02:03.000000Z\n" 
                << "3: Public keys have not been refreshed for too long period\n", 
            l->Stream.Str()); 
    } 
 
    Y_UNIT_TEST(Init) { 
        TPortManager pm; 
        ui16 port = pm.GetPort(80); 
        NMock::TMockServer server(port, []() { return new TTvmTool; }); 
 
        NTvmTool::TClientSettings s("push-client"); 
        s.SetAuthToken(AUTH_TOKEN); 
        s.SetPort(port); 
        s.SetHostname("localhost"); 
 
        auto l = MakeIntrusive<TLogger>(); 
        { 
            TAsyncUpdaterPtr u = TThreadedUpdater::Create(s, l); 
            UNIT_ASSERT_VALUES_EQUAL(TClientStatus::Ok, u->GetStatus()); 
        } 
        UNIT_ASSERT_VALUES_EQUAL( 
            TStringBuilder() 
                << "7: Meta info fetched from localhost:" << port << "\n" 
                << "6: Meta: self_tvm_id=100501, bb_env=ProdYateam, dsts=[(pass_likers:100502)]\n" 
                << "7: Tickets fetched from tvmtool: 2425-09-17T11:04:00.000000Z\n" 
                << "7: Public keys fetched from tvmtool: 2425-09-17T11:04:00.000000Z\n" 
                << "7: Thread-worker started\n" 
                << "7: Thread-worker stopped\n", 
            l->Stream.Str()); 
    } 
 
    Y_UNIT_TEST(InitWithoutTvmtool) { 
        NTvmTool::TClientSettings s("me"); 
        s.SetAuthToken(AUTH_TOKEN); 
        s.SetPort(0); 
 
        UNIT_ASSERT_EXCEPTION_CONTAINS(TThreadedUpdater::Create(s, TDevNullLogger::IAmBrave()), 
                                       TNonRetriableException, 
                                       "can not connect to "); 
    } 
 
    Y_UNIT_TEST(GetStatus) { 
        TNonInitedUpdater u("", 0, TDevNullLogger::IAmBrave()); 
        TMetaInfoProxy m(nullptr); 
        m.Config_ = std::make_shared<TMetaInfo::TConfig>(); 
        u.MetaInfo_ = m; 
        u.LastVisitForConfig_ = TInstant::Now(); 
 
        UNIT_ASSERT_VALUES_EQUAL(TClientStatus::Error, u.GetStatus()); 
        u.SetUpdateTimeOfPublicKeys(TInstant::Now() - TDuration::Days(3)); 
        UNIT_ASSERT_VALUES_EQUAL(TClientStatus::Warning, u.GetStatus()); 
        u.SetUpdateTimeOfPublicKeys(TInstant::Now() - TDuration::Hours(3)); 
        UNIT_ASSERT_VALUES_EQUAL(TClientStatus::Ok, u.GetStatus()); 
 
        u.SetServiceTickets(new TServiceTickets({}, {}, {})); 
 
        TMetaInfo::TConfig cfg; 
        cfg.DstAliases = {{"q", 1}, {"q2", 2}}; 
        m.Config_ = std::make_shared<TMetaInfo::TConfig>(cfg); 
        u.MetaInfo_ = m; 
        UNIT_ASSERT_VALUES_EQUAL(TClientStatus::Error, u.GetStatus()); 
        u.SetUpdateTimeOfServiceTickets(TInstant::Now() - TDuration::Hours(3)); 
        UNIT_ASSERT_VALUES_EQUAL(TClientStatus::Error, u.GetStatus()); 
 
        u.SetServiceTickets(MakeIntrusiveConst<TServiceTickets>( 
            TServiceTickets::TMapIdStr({{1, "3:serv:CBAQ__________9_IgYIKhCUkQY:CX"}, {2, "t"}}), 
            TServiceTickets::TMapIdStr({{3, "mega_error"}, {4, "error2"}}), 
            TServiceTickets::TMapAliasId({ 
                {"some_alias#1", 1}, 
                {"some_alias#2", 2}, 
            }))); 
        UNIT_ASSERT_VALUES_EQUAL(TClientStatus::Warning, u.GetStatus()); 
 
        const TInstant* inv = &u.GetCachedServiceTickets()->InvalidationTime; 
        *const_cast<TInstant*>(inv) = TInstant::Now() + TDuration::Hours(3); 
        UNIT_ASSERT_VALUES_EQUAL(TClientStatus::Warning, u.GetStatus()); 
 
        u.SetUpdateTimeOfServiceTickets(TInstant::Now() - TDuration::Minutes(3)); 
        UNIT_ASSERT_VALUES_EQUAL(TClientStatus::Ok, u.GetStatus()); 
 
        u.LastVisitForConfig_ = TInstant::Now() - TDuration::Minutes(1); 
        UNIT_ASSERT_VALUES_EQUAL(TClientStatus::Warning, u.GetStatus()); 
    } 
 
    Y_UNIT_TEST(multiNamesForDst) { 
        TPortManager pm; 
        ui16 port = pm.GetPort(80); 
        NMock::TMockServer server(port, []() { return new TTvmTool; }); 
 
        NTvmTool::TClientSettings s("multi_names_for_dst"); 
        s.SetAuthToken(AUTH_TOKEN); 
        s.SetPort(port); 
        s.SetHostname("localhost"); 
 
        auto l = MakeIntrusive<TLogger>(); 
        { 
            TAsyncUpdaterPtr u = TThreadedUpdater::Create(s, l); 
            UNIT_ASSERT_VALUES_EQUAL(TClientStatus::Ok, u->GetStatus()); 
        } 
        UNIT_ASSERT_VALUES_EQUAL( 
            TStringBuilder() 
                << "7: Meta info fetched from localhost:" << port << "\n" 
                << "6: Meta: self_tvm_id=100599, bb_env=ProdYateam, dsts=[(pass_haters:100502)(pass_likers:100502)]\n" 
                << "7: Tickets fetched from tvmtool: 2425-09-17T11:04:00.000000Z\n" 
                << "7: Public keys fetched from tvmtool: 2425-09-17T11:04:00.000000Z\n" 
                << "7: Thread-worker started\n" 
                << "7: Thread-worker stopped\n", 
            l->Stream.Str()); 
    } 
} 
