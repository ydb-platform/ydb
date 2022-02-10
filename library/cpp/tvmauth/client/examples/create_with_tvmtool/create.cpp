#include <library/cpp/tvmauth/client/facade.h> 
 
namespace NExample { 
    // Possibility of using functions depends on config of tvmtool 
    //    CheckServiceTicket 
    //    CheckUserTicket 
    //    GetServiceTicketFor 
 
    NTvmAuth::TTvmClient CreateClientInQloudOrYandexDeploy() { 
        NTvmAuth::NTvmTool::TClientSettings setts( 
            "my_service" // specified in Qloud/YP/tvmtool interface 
        ); 
 
        NTvmAuth::TLoggerPtr log = MakeIntrusive<NTvmAuth::TCerrLogger>(7); 
 
        NTvmAuth::TTvmClient c(setts, log); 
 
        return c; 
    } 
 
    NTvmAuth::TTvmClient CreateClientForDevOrTests() { 
        NTvmAuth::NTvmTool::TClientSettings setts( 
            "my_service" // specified in Qloud/YP/tvmtool interface 
        ); 
        setts.SetPort(18080); 
        setts.SetAuthToken("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"); 
 
        NTvmAuth::TLoggerPtr log = MakeIntrusive<NTvmAuth::TCerrLogger>(7); 
 
        NTvmAuth::TTvmClient c(setts, log); 
 
        return c; 
    } 
} 
