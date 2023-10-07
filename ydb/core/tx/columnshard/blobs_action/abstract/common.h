#pragma once
#include <util/system/types.h>
#include <ydb/library/accessor/accessor.h>
#include <ydb/library/services/services.pb.h>
#include <util/generic/string.h>
#include <util/generic/guid.h>

namespace NKikimr::NOlap {

class ICommonBlobsAction {
private:
    YDB_READONLY_DEF(TString, StorageId);
    YDB_READONLY(TString, ActionGuid, TGUID::CreateTimebased().AsGuidString());
    const i64 ActionId = 0;
public:
    i64 GetActionId() const {
        return ActionId;
    }

    ICommonBlobsAction(const TString& storageId);
    virtual ~ICommonBlobsAction() = default;
};

}
