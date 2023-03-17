#pragma once

#include <util/generic/string.h>
#include <ydb/core/protos/external_sources.pb.h>
#include <ydb/library/yql/public/issue/yql_issue.h>

namespace NKikimr::NExternalSource {

struct TExternalSourceException: public yexception {
};

struct IExternalSource : public TThrRefBase {
    using TPtr = TIntrusivePtr<IExternalSource>;

    virtual TString Pack(const NKikimrExternalSources::TSchema& schema,
                         const NKikimrExternalSources::TGeneral& general) const = 0;
};

}
