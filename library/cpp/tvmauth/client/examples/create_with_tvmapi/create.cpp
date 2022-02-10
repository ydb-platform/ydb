#include <library/cpp/tvmauth/client/facade.h> 
 
namespace NExample { 
    NTvmAuth::TTvmClient CreateClientForCheckingAllTicketsAndFetchingServiceTickets() { 
        NTvmAuth::NTvmApi::TClientSettings setts{ 
            .DiskCacheDir = "/var/cache/my_service/tvm/", 
            .SelfTvmId = 11, 
            .Secret = (TStringBuf) "AAAAAAAAAAAAAAAAAAAAAA", 
            .FetchServiceTicketsForDstsWithAliases = { 
                {"bb", 224}, 
                {"datasync", 2000060}, 
            }, 
            .CheckServiceTickets = true, 
            .CheckUserTicketsWithBbEnv = NTvmAuth::EBlackboxEnv::Test, 
        }; 
 
        NTvmAuth::TLoggerPtr log = MakeIntrusive<NTvmAuth::TCerrLogger>(7); 
 
        NTvmAuth::TTvmClient c(setts, log); 
 
        // c.CheckServiceTicket("some service ticket") 
        // c.CheckUserTicket("some user ticket") 
        // c.GetServiceTicketFor("bb") 
        // c.GetServiceTicketFor(224) 
 
        return c; 
    } 
 
    NTvmAuth::TTvmClient CreateClientForCheckingAllTickets() { 
        NTvmAuth::NTvmApi::TClientSettings setts{ 
            .DiskCacheDir = "/var/cache/my_service/tvm/", 
            .SelfTvmId = 11, 
            .CheckServiceTickets = true, 
            .CheckUserTicketsWithBbEnv = NTvmAuth::EBlackboxEnv::Test, 
        }; 
 
        NTvmAuth::TLoggerPtr log = MakeIntrusive<NTvmAuth::TCerrLogger>(7); 
 
        NTvmAuth::TTvmClient c(setts, log); 
 
        // c.CheckServiceTicket("some service ticket") 
        // c.CheckUserTicket("some user ticket") 
 
        return c; 
    } 
 
    NTvmAuth::TTvmClient CreateClientForFetchingServiceTickets() { 
        NTvmAuth::NTvmApi::TClientSettings setts{ 
            .DiskCacheDir = "/var/cache/my_service/tvm/", 
            .SelfTvmId = 11, 
            .Secret = (TStringBuf) "AAAAAAAAAAAAAAAAAAAAAA", 
            .FetchServiceTicketsForDstsWithAliases = { 
                {"bb", 224}, 
                {"datasync", 2000060}, 
            }, 
        }; 
 
        NTvmAuth::TLoggerPtr log = MakeIntrusive<NTvmAuth::TCerrLogger>(7); 
 
        NTvmAuth::TTvmClient c(setts, log); 
 
        // c.GetServiceTicketFor("bb") 
        // c.GetServiceTicketFor(224) 
 
        return c; 
    } 
 
    NTvmAuth::TTvmClient CreateClientForCheckingServiceTickets() { 
        NTvmAuth::NTvmApi::TClientSettings setts{ 
            .DiskCacheDir = "/var/cache/my_service/tvm/", 
            .SelfTvmId = 11, 
            .CheckServiceTickets = true, 
        }; 
 
        NTvmAuth::TLoggerPtr log = MakeIntrusive<NTvmAuth::TCerrLogger>(7); 
 
        NTvmAuth::TTvmClient c(setts, log); 
 
        // c.CheckServiceTicket("some service ticket") 
 
        return c; 
    } 
 
    NTvmAuth::TTvmClient CreateClientForCheckingServiceTicketsWithRoles() { 
        NTvmAuth::NTvmApi::TClientSettings setts{ 
            .DiskCacheDir = "/var/cache/my_service/tvm/", 
            .SelfTvmId = 11, 
            .Secret = (TStringBuf) "AAAAAAAAAAAAAAAAAAAAAA", 
            .CheckServiceTickets = true, 
            .FetchRolesForIdmSystemSlug = "passporttestservice", 
        }; 
 
        NTvmAuth::TLoggerPtr log = MakeIntrusive<NTvmAuth::TCerrLogger>(7); 
 
        NTvmAuth::TTvmClient c(setts, log); 
 
        // auto t = c.CheckServiceTicket("some service ticket") 
        // c.GetRoles()->CheckServiceRole(t, "some role"); 
 
        return c; 
    } 
} 
