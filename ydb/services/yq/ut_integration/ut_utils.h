#pragma once

#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>
#include <util/system/shellcommand.h>
#include <contrib/libs/curl/include/curl/curl.h>
#include <library/cpp/json/json_reader.h>

void UpsertToExistingTable(NYdb::TDriver& driver, const TString& location);

