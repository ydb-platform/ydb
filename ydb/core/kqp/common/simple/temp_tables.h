#pragma once

#include <ydb/library/aclib/aclib.h>

#include <optional>

#include <util/generic/fwd.h>
#include <util/generic/hash.h>
#include <util/generic/vector.h>

namespace NKikimr::NKqp {

struct TKqpTempTablesState {
    struct TTempTableInfo {
        TString Name;
        TString WorkingDir;
        TString Database;
        TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
        TString Cluster;
    };
    std::optional<TString> SessionId;
    THashMap<std::pair<TString, TString>, TTempTableInfo> TempTables;

    using TConstPtr = std::shared_ptr<const TKqpTempTablesState>;
};

} // namespace NKikimr::NKqp
