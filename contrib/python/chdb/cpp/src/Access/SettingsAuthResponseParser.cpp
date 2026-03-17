#include "SettingsAuthResponseParser.h"

#include <Access/resolveSetting.h>
#include <IO/HTTPCommon.h>

#include <CHDBPoco/JSON/Object.h>
#include <CHDBPoco/JSON/Parser.h>
#include <CHDBPoco/Timespan.h>

namespace DB_CHDB
{

SettingsAuthResponseParser::Result
SettingsAuthResponseParser::parse(const CHDBPoco::Net::HTTPResponse & response, std::istream * body_stream) const
{
    Result result;

    if (response.getStatus() != CHDBPoco::Net::HTTPResponse::HTTPStatus::HTTP_OK)
        return result;
    result.is_ok = true;

    if (!body_stream)
        return result;

    CHDBPoco::JSON::Parser parser;
    CHDBPoco::JSON::Object::Ptr parsed_body;

    try
    {
        CHDBPoco::Dynamic::Var json = parser.parse(*body_stream);
        CHDBPoco::JSON::Object::Ptr obj = json.extract<CHDBPoco::JSON::Object::Ptr>();
        CHDBPoco::JSON::Object::Ptr settings_obj = obj->getObject(settings_key);

        if (settings_obj)
            for (const auto & [key, value] : *settings_obj)
                result.settings.emplace_back(key, settingStringToValueUtil(key, value));
    }
    catch (...)
    {
        LOG_INFO(getLogger("HTTPAuthentication"), "Failed to parse settings from authentication response. Skip it.");
    }
    return result;
}

}
