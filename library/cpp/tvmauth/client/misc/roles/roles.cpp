#include "roles.h" 
 
#include <library/cpp/tvmauth/checked_service_ticket.h> 
#include <library/cpp/tvmauth/checked_user_ticket.h> 
 
namespace NTvmAuth::NRoles { 
    TRoles::TRoles(TMeta&& meta, 
                   TTvmConsumers tvm, 
                   TUserConsumers user, 
                   TRawPtr raw) 
        : Meta_(std::move(meta)) 
        , TvmIds_(std::move(tvm)) 
        , Users_(std::move(user)) 
        , Raw_(std::move(raw)) 
    { 
        Y_ENSURE(Raw_); 
    } 
 
    TConsumerRolesPtr TRoles::GetRolesForService(const TCheckedServiceTicket& t) const { 
        Y_ENSURE_EX(t, 
                    TIllegalUsage() << "Service ticket must be valid, got: " << t.GetStatus()); 
        auto it = TvmIds_.find(t.GetSrc()); 
        return it == TvmIds_.end() ? TConsumerRolesPtr() : it->second; 
    } 
 
    TConsumerRolesPtr TRoles::GetRolesForUser(const TCheckedUserTicket& t, 
                                              std::optional<TUid> selectedUid) const { 
        Y_ENSURE_EX(t, 
                    TIllegalUsage() << "User ticket must be valid, got: " << t.GetStatus()); 
        Y_ENSURE_EX(t.GetEnv() == EBlackboxEnv::ProdYateam, 
                    TIllegalUsage() << "User ticket must be from ProdYateam, got from " << t.GetEnv()); 
 
        TUid uid = t.GetDefaultUid(); 
        if (selectedUid) { 
            auto it = std::find(t.GetUids().begin(), t.GetUids().end(), *selectedUid); 
            Y_ENSURE_EX(it != t.GetUids().end(), 
                        TIllegalUsage() << "selectedUid must be in user ticket but it's not: " 
                                        << *selectedUid); 
            uid = *selectedUid; 
        } 
 
        auto it = Users_.find(uid); 
        return it == Users_.end() ? TConsumerRolesPtr() : it->second; 
    } 
 
    const TRoles::TMeta& TRoles::GetMeta() const { 
        return Meta_; 
    } 
 
    const TString& TRoles::GetRaw() const { 
        return *Raw_; 
    } 
 
    bool TRoles::CheckServiceRole(const TCheckedServiceTicket& t, 
                                  const TStringBuf roleName) const { 
        TConsumerRolesPtr c = GetRolesForService(t); 
        return c ? c->HasRole(roleName) : false; 
    } 
 
    bool TRoles::CheckUserRole(const TCheckedUserTicket& t, 
                               const TStringBuf roleName, 
                               std::optional<TUid> selectedUid) const { 
        TConsumerRolesPtr c = GetRolesForUser(t, selectedUid); 
        return c ? c->HasRole(roleName) : false; 
    } 
 
    bool TRoles::CheckServiceRoleForExactEntity(const TCheckedServiceTicket& t, 
                                                const TStringBuf roleName, 
                                                const TEntity& exactEntity) const { 
        TConsumerRolesPtr c = GetRolesForService(t); 
        return c ? c->CheckRoleForExactEntity(roleName, exactEntity) : false; 
    } 
 
    bool TRoles::CheckUserRoleForExactEntity(const TCheckedUserTicket& t, 
                                             const TStringBuf roleName, 
                                             const TEntity& exactEntity, 
                                             std::optional<TUid> selectedUid) const { 
        TConsumerRolesPtr c = GetRolesForUser(t, selectedUid); 
        return c ? c->CheckRoleForExactEntity(roleName, exactEntity) : false; 
    } 
 
    TConsumerRoles::TConsumerRoles(THashMap<TString, TEntitiesPtr> roles) 
        : Roles_(std::move(roles)) 
    { 
    } 
 
    bool TConsumerRoles::CheckRoleForExactEntity(const TStringBuf roleName, 
                                                 const TEntity& exactEntity) const { 
        auto it = Roles_.find(roleName); 
        if (it == Roles_.end()) { 
            return false; 
        } 
 
        return it->second->Contains(exactEntity); 
    } 
 
    TEntities::TEntities(TEntitiesIndex idx) 
        : Idx_(std::move(idx)) 
    { 
    } 
} 
