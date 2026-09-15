#pragma once

#include <util/generic/maybe.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>

namespace NYql::NFmr {

using TTvmId = ui64;

class IFmrTvmClient: public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<IFmrTvmClient>;

    virtual ~IFmrTvmClient() = default;

    virtual TString MakeTvmServiceTicket(TTvmId destinationTvmId) = 0;

    virtual void CheckTvmServiceTicket(const TString& serviceTicket, const std::vector<TTvmId>& allowedSourceTvmIds) = 0;
};

} // namespace NYql::NFmr
