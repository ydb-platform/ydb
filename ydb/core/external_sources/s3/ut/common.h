#pragma once

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

namespace NKikimr::NKqp {

TString Exec(const TString& cmd);

TString GetExternalPort(const TString& service, const TString& port);

void WaitBucket(std::shared_ptr<TKikimrRunner> kikimr, const TString& externalDataSourceName);

} // namespace NKikimr::NKqp
