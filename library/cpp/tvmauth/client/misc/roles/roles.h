#pragma once 
 
#include "entities_index.h" 
#include "types.h" 
 
#include <library/cpp/tvmauth/client/exception.h> 
 
#include <library/cpp/tvmauth/type.h> 
 
#include <util/datetime/base.h> 
#include <util/generic/array_ref.h> 
#include <util/generic/hash.h> 
 
#include <vector> 
 
namespace NTvmAuth { 
    class TCheckedServiceTicket; 
    class TCheckedUserTicket; 
} 
 
namespace NTvmAuth::NRoles { 
    class TRoles { 
    public: 
        struct TMeta { 
            TString Revision; 
            TInstant BornTime; 
            TInstant Applied = TInstant::Now(); 
        }; 
 
        using TTvmConsumers = THashMap<TTvmId, TConsumerRolesPtr>; 
        using TUserConsumers = THashMap<TUid, TConsumerRolesPtr>; 
 
        TRoles(TMeta&& meta, 
               TTvmConsumers tvm, 
               TUserConsumers user, 
               TRawPtr raw); 
 
        /** 
         * @return ptr to roles. It will be nullptr if there are no roles 
         */ 
        TConsumerRolesPtr GetRolesForService(const TCheckedServiceTicket& t) const; 
 
        /** 
         * @return ptr to roles. It will be nullptr if there are no roles 
         */ 
        TConsumerRolesPtr GetRolesForUser(const TCheckedUserTicket& t, 
                                          std::optional<TUid> selectedUid = {}) const; 
 
        const TMeta& GetMeta() const; 
        const TString& GetRaw() const; 
 
    public: // shortcuts 
        /** 
         * @brief CheckServiceRole() is shortcut for simple role checking - for any possible entity 
         */ 
        bool CheckServiceRole( 
            const TCheckedServiceTicket& t, 
            const TStringBuf roleName) const; 
 
        /** 
         * @brief CheckUserRole() is shortcut for simple role checking - for any possible entity 
         */ 
        bool CheckUserRole( 
            const TCheckedUserTicket& t, 
            const TStringBuf roleName, 
            std::optional<TUid> selectedUid = {}) const; 
 
        /** 
         * @brief CheckServiceRoleForExactEntity() is shortcut for simple role checking for exact entity 
         */ 
        bool CheckServiceRoleForExactEntity( 
            const TCheckedServiceTicket& t, 
            const TStringBuf roleName, 
            const TEntity& exactEntity) const; 
 
        /** 
         * @brief CheckUserRoleForExactEntity() is shortcut for simple role checking for exact entity 
         */ 
        bool CheckUserRoleForExactEntity( 
            const TCheckedUserTicket& t, 
            const TStringBuf roleName, 
            const TEntity& exactEntity, 
            std::optional<TUid> selectedUid = {}) const; 
 
    private: 
        TMeta Meta_; 
        TTvmConsumers TvmIds_; 
        TUserConsumers Users_; 
        TRawPtr Raw_; 
    }; 
 
    class TConsumerRoles { 
    public: 
        TConsumerRoles(THashMap<TString, TEntitiesPtr> roles); 
 
        bool HasRole(const TStringBuf roleName) const { 
            return Roles_.contains(roleName); 
        } 
 
        /** 
         * @return ptr to entries. It will be nullptr if there is no role 
         */ 
        TEntitiesPtr GetEntitiesForRole(const TStringBuf roleName) const { 
            auto it = Roles_.find(roleName); 
            return it == Roles_.end() ? TEntitiesPtr() : it->second; 
        } 
 
        /** 
         * @brief CheckRoleForExactEntity() is shortcut for simple role checking for exact entity 
         */ 
        bool CheckRoleForExactEntity(const TStringBuf roleName, 
                                     const TEntity& exactEntity) const; 
 
    private: 
        THashMap<TString, TEntitiesPtr> Roles_; 
    }; 
 
    class TEntities { 
    public: 
        TEntities(TEntitiesIndex idx); 
 
        /** 
         * @brief Contains() provides info about entity presence 
         */ 
        bool Contains(const TEntity& exactEntity) const { 
            return Idx_.ContainsExactEntity(exactEntity.begin(), exactEntity.end()); 
        } 
 
        /** 
         * @brief The same as Contains() 
         * It checks span for sorted and unique properties. 
         */ 
        template <class StrKey = TString, class StrValue = TString> 
        bool ContainsSortedUnique( 
            const TArrayRef<const std::pair<StrKey, StrValue>>& exactEntity) const { 
            CheckSpan(exactEntity); 
            return Idx_.ContainsExactEntity(exactEntity.begin(), exactEntity.end()); 
        } 
 
        /** 
         * @brief GetEntitiesWithAttrs() collects entities with ALL attributes from `attrs` 
         */ 
        template <class StrKey = TString, class StrValue = TString> 
        const std::vector<TEntityPtr>& GetEntitiesWithAttrs( 
            const std::map<StrKey, StrValue>& attrs) const { 
            return Idx_.GetEntitiesWithAttrs(attrs.begin(), attrs.end()); 
        } 
 
        /** 
         * @brief The same as GetEntitiesWithAttrs() 
         * It checks span for sorted and unique properties. 
         */ 
        template <class StrKey = TString, class StrValue = TString> 
        const std::vector<TEntityPtr>& GetEntitiesWithSortedUniqueAttrs( 
            const TArrayRef<const std::pair<StrKey, StrValue>>& attrs) const { 
            CheckSpan(attrs); 
            return Idx_.GetEntitiesWithAttrs(attrs.begin(), attrs.end()); 
        } 
 
    private: 
        template <class StrKey, class StrValue> 
        static void CheckSpan(const TArrayRef<const std::pair<StrKey, StrValue>>& attrs) { 
            if (attrs.empty()) { 
                return; 
            } 
 
            auto prev = attrs.begin(); 
            for (auto it = prev + 1; it != attrs.end(); ++it) { 
                Y_ENSURE_EX(prev->first != it->first, 
                            TIllegalUsage() << "attrs are not unique: '" << it->first << "'"); 
                Y_ENSURE_EX(prev->first < it->first, 
                            TIllegalUsage() << "attrs are not sorted: '" << prev->first 
                                            << "' before '" << it->first << "'"); 
 
                prev = it; 
            } 
        } 
 
    private: 
        TEntitiesIndex Idx_; 
    }; 
} 
