#pragma once

#include <ydb/library/aclib/aclib.h>

#include <optional>
#include <string_view>

#include <util/generic/fwd.h>
#include <util/generic/hash.h>
#include <util/generic/vector.h>

namespace NKikimr::NKqp {

struct TKqpTempTablesState {
    struct TTempTableInfo {
        TString Name;
        TString WorkingDir;
        TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    };
    TString SessionId;
    THashMap<TString, TTempTableInfo> TempTables;

    using TConstPtr = std::shared_ptr<const TKqpTempTablesState>;

    THashMap<TString, TTempTableInfo>::const_iterator
    FindInfo(const std::string_view& path, bool withSessionId = false) const;
};

} // namespace NKikimr::NKqp
