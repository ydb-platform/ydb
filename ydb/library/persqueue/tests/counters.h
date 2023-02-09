#pragma once

#include <library/cpp/json/json_value.h>


namespace NKikimr::NPersQueueTests {

NJson::TJsonValue SendQuery(ui16 port, const TString& query, bool mayFail = false);

NJson::TJsonValue GetCountersLegacy(ui16 port, const TString& counters, const TString& subsystem,
                                    const TString& topicPath, const TString& clientDc = "",
                                    const TString& originalDc = "", const TString& client = "",
                                    const TString& consumerPath = "");

NJson::TJsonValue GetClientCountersLegacy(ui16 port, const TString& counters, const TString& subsystem,
                                          const TString& client, const TString& consumerPath);

NJson::TJsonValue GetCounters1stClass(ui16 port, const TString& counters,
                                      const TString& databasePath,
                                      const TString& cloudId, const TString& databaseId,
                                      const TString& folderId, const TString& topicName,
                                      const TString& consumer, const TString& host,
                                      const TString& partition);

} // NKikimr::NPersQueueTests
