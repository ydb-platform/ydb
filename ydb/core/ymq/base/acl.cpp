#include "acl.h"

#include <util/generic/hash.h>
#include <util/generic/singleton.h>
#include <util/generic/string.h>

#include <map>

namespace NKikimr::NSQS {

class TSQSACLMappings {
public:
    TSQSACLMappings() {
        static const TStringBuf createQueue = "CreateQueue";
        static const TStringBuf createUser = "CreateUser";
        static const TStringBuf sendMessage = "SendMessage";
        static const TStringBuf receiveMessage = "ReceiveMessage";
        static const TStringBuf deleteMessage = "DeleteMessage";
        static const TStringBuf modifyPermissions = "ModifyPermissions";
        static const TStringBuf alterQueue = "AlterQueue";
        static const TStringBuf describePath = "DescribePath";
        static const TStringBuf deleteUser = "DeleteUser";
        static const TStringBuf deleteQueue = "DeleteQueue";

        ACE2AccessIndex = {
             {createQueue, NACLib::EAccessRights::CreateQueue},
             {createUser, NACLib::EAccessRights::CreateTable},
             {sendMessage, NACLib::EAccessRights::UpdateRow},
             {receiveMessage, NACLib::EAccessRights::SelectRow},
             {deleteMessage, NACLib::EAccessRights::EraseRow},
             {modifyPermissions, NACLib::EAccessRights::WriteAttributes},
             {alterQueue, NACLib::EAccessRights::AlterSchema},
             {describePath, NACLib::EAccessRights::DescribeSchema},
             {deleteUser, NACLib::EAccessRights::RemoveSchema},
             {deleteQueue, NACLib::EAccessRights::ReadAttributes}}; // as intended

       Action2ACEIndex = {
            {"CreateQueue", {EACLSourceType::AccountDir, createQueue}},
            {"CreateUser", {EACLSourceType::RootDir, createUser}},
            {"SendMessage", {EACLSourceType::QueueDir, sendMessage}},
            {"SendMessageBatch", {EACLSourceType::QueueDir, sendMessage}},
            {"ChangeMessageVisibility", {EACLSourceType::QueueDir, receiveMessage}},
            {"ChangeMessageVisibilityBatch", {EACLSourceType::QueueDir, receiveMessage}},
            {"ReceiveMessage", {EACLSourceType::QueueDir, receiveMessage}},
            {"DeleteMessage", {EACLSourceType::QueueDir, deleteMessage}},
            {"DeleteMessageBatch", {EACLSourceType::QueueDir, deleteMessage}},
            {"PurgeQueue", {EACLSourceType::QueueDir, deleteMessage}},
            {"ModifyPermissions", {EACLSourceType::Custom, modifyPermissions}},
            {"SetQueueAttributes", {EACLSourceType::QueueDir, alterQueue}},
            {"TagQueue", {EACLSourceType::QueueDir, alterQueue}},
            {"UntagQueue", {EACLSourceType::QueueDir, alterQueue}},
            {"GetQueueAttributes", {EACLSourceType::QueueDir, describePath}},
            {"ListQueueTags", {EACLSourceType::QueueDir, describePath}},
            {"GetQueueUrl", {EACLSourceType::QueueDir, describePath}},
            {"ListQueues", {EACLSourceType::AccountDir, describePath}},
            {"ListDeadLetterSourceQueues", {EACLSourceType::QueueDir, describePath}},
            {"ListPermissions", {EACLSourceType::Custom, describePath}},
            {"CountQueues", {EACLSourceType::AccountDir, describePath}},
            {"DeleteUser", {EACLSourceType::AccountDir, deleteUser}},
            {"DeleteQueue", {EACLSourceType::QueueDir, deleteQueue}},
            {"ListQueueTags", {EACLSourceType::QueueDir, describePath}},
            {"TagQueue", {EACLSourceType::QueueDir, alterQueue}},
            {"UntagQueue", {EACLSourceType::QueueDir, alterQueue}},
        };

        for (const auto& pair : ACE2AccessIndex) {
            Access2ACEIndex[pair.second] = pair.first;
        }

        Y_ABORT_UNLESS(Access2ACEIndex.size() == ACE2AccessIndex.size());
    }

    EACLSourceType GetActionACLSourceTypeImpl(const TStringBuf action) const {
        auto it = Action2ACEIndex.find(action);
        if (it != Action2ACEIndex.end()) {
            return it->second.first;
        }

        return EACLSourceType::Unknown;
    }

    ui32 GetActionRequiredAccessImpl(const TStringBuf action) const {
        return GetACERequiredAccessImpl(GetActionMatchingACEImpl(action));
    }

    ui32 GetACERequiredAccessImpl(const TStringBuf aceName) const {
        auto accessIt = ACE2AccessIndex.find(aceName);
        if (accessIt != ACE2AccessIndex.end()) {
            return accessIt->second;
        }

        return 0;
    }

    TString GetActionMatchingACEImpl(const TStringBuf actionName) const {
        auto it = Action2ACEIndex.find(actionName);
        if (it != Action2ACEIndex.end()) {
            return TString(it->second.second);
        }

        return {};
    }

    TVector<TStringBuf> GetAccessMatchingACEImpl(const ui32 access) const {
        TVector<TStringBuf> result;
        for (const auto& pair : Access2ACEIndex) {
            if (pair.first & access) {
                result.push_back(pair.second);
            }
        }

        return result;
    }

private:
    THashMap<TStringBuf, ui32> ACE2AccessIndex;
    THashMap<ui32, TStringBuf> Access2ACEIndex;
    THashMap<TStringBuf, std::pair<EACLSourceType, TStringBuf>> Action2ACEIndex;
};

static const TSQSACLMappings& Mappings() {
    return *Singleton<TSQSACLMappings>();
}

EACLSourceType GetActionACLSourceType(const TString& actionName) {
    return Mappings().GetActionACLSourceTypeImpl(actionName);
}

ui32 GetActionRequiredAccess(const TString& actionName) {
    return Mappings().GetActionRequiredAccessImpl(actionName);
}

ui32 GetACERequiredAccess(const TString& aceName) {
    return Mappings().GetACERequiredAccessImpl(aceName);
}

TString GetActionMatchingACE(const TString& actionName) {
    return Mappings().GetActionMatchingACEImpl(actionName);
}

TVector<TStringBuf> GetAccessMatchingACE(const ui32 access) {
    return Mappings().GetAccessMatchingACEImpl(access);
}

} // namespace NKikimr::NSQS
