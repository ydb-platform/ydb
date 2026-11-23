#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>

#include <contrib/libs/curl/include/curl/curl.h>
#include <library/cpp/json/json_reader.h>
#include <util/system/shellcommand.h>

void UpsertToExistingTable(NYdb::TDriver& driver, const TString& location);
