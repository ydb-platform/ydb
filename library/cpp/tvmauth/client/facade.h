#pragma once 
 
#include "misc/async_updater.h" 
#include "misc/api/settings.h" 
#include "misc/tool/settings.h" 
 
#include <library/cpp/tvmauth/checked_service_ticket.h> 
#include <library/cpp/tvmauth/checked_user_ticket.h> 
 
namespace NTvmAuth::NInternal { 
    class TClientCaningKnife; 
} 
 
namespace NTvmAuth { 
    class TDefaultUidChecker; 
    class TServiceTicketGetter; 
    class TServiceTicketChecker; 
    class TSrcChecker; 
    class TUserTicketChecker; 
 
    /*! 
     * Long lived thread-safe object for interacting with TVM. 
     * In 99% cases TvmClient shoud be created at service startup and live for the whole process lifetime. 
     */ 
    class TTvmClient { 
    public: 
        /*! 
         * Uses local http-interface to get state: http://localhost/tvm/. 
         * This interface can be provided with tvmtool (local daemon) or Qloud/YP (local http api in container). 
         * See more: https://wiki.yandex-team.ru/passport/tvm2/tvm-daemon/. 
         * 
         * Starts thread for updating of in-memory cache in background 
         * @param settings 
         * @param logger is usefull for monitoring and debuging 
         */ 
        TTvmClient(const NTvmTool::TClientSettings& settings, TLoggerPtr logger); 
 
        /*! 
         * Uses general way to get state: https://tvm-api.yandex.net. 
         * It is not recomended for Qloud/YP. 
         * 
         * Starts thread for updating of in-memory cache in background 
         * Reads cache from disk if specified 
         * @param settings 
         * @param logger is usefull for monitoring and debuging 
         */ 
        TTvmClient(const NTvmApi::TClientSettings& settings, TLoggerPtr logger); 
 
        /*! 
         * Feel free to use custom updating logic in tests 
         */ 
        TTvmClient(TAsyncUpdaterPtr updater); 
 
        TTvmClient(TTvmClient&&); 
        ~TTvmClient(); 
        TTvmClient& operator=(TTvmClient&&); 
 
        /*! 
         * You should trigger your monitoring if status is not Ok. 
         * It will be unable to operate if status is Error. 
         * Description: https://a.yandex-team.ru/arc/trunk/arcadia/library/cpp/tvmauth/client/README.md#high-level-interface 
         * @return Current status of client. 
         */ 
        TClientStatus GetStatus() const; 
 
        /*! 
         * Some tools for monitoring 
         */ 
 
        TInstant GetUpdateTimeOfPublicKeys() const; 
        TInstant GetUpdateTimeOfServiceTickets() const; 
        TInstant GetInvalidationTimeOfPublicKeys() const; 
        TInstant GetInvalidationTimeOfServiceTickets() const; 
 
        /*! 
         * Requires fetchinig options (from TClientSettings or Qloud/YP/tvmtool settings) 
         * Can throw exception if cache is invalid or wrong config 
         * 
         * Alias is local label for TvmID 
         *  which can be used to avoid this number in every checking case in code. 
         * @param dst 
         */ 
        TString GetServiceTicketFor(const TClientSettings::TAlias& dst) const; 
        TString GetServiceTicketFor(const TTvmId dst) const; 
 
        /*! 
         * For TTvmApi::TClientSettings: checking must be enabled in TClientSettings 
         * Can throw exception if checking was not enabled in settings 
         * 
         * ServiceTicket contains src: you should check it by yourself with ACL 
         * @param ticket 
         */ 
        TCheckedServiceTicket CheckServiceTicket(TStringBuf ticket) const; 
 
        /*! 
         * Requires blackbox enviroment (from TClientSettings or Qloud/YP/tvmtool settings) 
         * Can throw exception if checking was not enabled in settings 
         * @param ticket 
         * @param overrideEnv allowes you to override env from settings 
         */ 
        TCheckedUserTicket CheckUserTicket(TStringBuf ticket, TMaybe<EBlackboxEnv> overrideEnv = {}) const; 
 
        /*! 
         * Under construction now. It is unusable. 
         * PASSP-30283 
         */ 
        NRoles::TRolesPtr GetRoles() const; 
 
    private: 
        TAsyncUpdaterPtr Updater_; 
        THolder<TServiceTicketGetter> Tickets_; 
        THolder<TServiceTicketChecker> Service_; 
        THolder<TUserTicketChecker> User_; 
        THolder<TSrcChecker> SrcChecker_; 
        THolder<TDefaultUidChecker> DefaultUidChecker_; 
 
        friend class NInternal::TClientCaningKnife; 
    }; 
} 
