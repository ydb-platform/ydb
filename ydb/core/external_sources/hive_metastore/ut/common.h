#include <util/generic/string.h>

namespace NKikimr::NExternalSource {

TString GetExternalPort(const TString& service, const TString& port);

void WaitHiveMetastore(const TString& host, int32_t port, const TString& database);

}
