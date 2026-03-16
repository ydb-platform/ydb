#pragma once

#include <IO/ReadWriteBufferFromHTTP.h>
#include <functional>
#include <DBPoco/JSON/Parser.h>

namespace DataLake
{

DB::ReadWriteBufferFromHTTPPtr createReadBuffer(
    const std::string & endpoint,
    DB::ContextPtr context,
    const DBPoco::Net::HTTPBasicCredentials & credentials,
    const DBPoco::URI::QueryParameters & params = {},
    const DB::HTTPHeaderEntries & headers = {},
    const std::string & method = DBPoco::Net::HTTPRequest::HTTP_GET,
    std::function<void(std::ostream &)> out_stream_callaback = {});

std::pair<DBPoco::Dynamic::Var, std::string> makeHTTPRequestAndReadJSON(
    const std::string & endpoint,
    DB::ContextPtr context,
    const DBPoco::Net::HTTPBasicCredentials & credentials,
    const DBPoco::URI::QueryParameters & params = {},
    const DB::HTTPHeaderEntries & headers = {},
    const std::string & method = DBPoco::Net::HTTPRequest::HTTP_GET,
    std::function<void(std::ostream &)> out_stream_callaback = {});

}
