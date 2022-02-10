#pragma once

#include <ydb/core/scheme/scheme_tabledefs.h>

namespace NKikimr {
namespace NSysView {

struct TDbWatcherCallback : public TThrRefBase {
    virtual void OnDatabaseRemoved(const TString& database, TPathId pathId) = 0;
};

IActor* CreateDbWatcherActor(TIntrusivePtr<TDbWatcherCallback> callback);

}
}
