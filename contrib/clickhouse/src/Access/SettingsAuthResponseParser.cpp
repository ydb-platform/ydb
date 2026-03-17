#include "SettingsAuthResponseParser.h"

#include <Access/resolveSetting.h>
#include <IO/HTTPCommon.h>

#include <DBPoco/JSON/Object.h>
#include <DBPoco/JSON/Parser.h>
#include <DBPoco/Timespan.h>

namespace DB
{

SettingsAuthResponseParser::Result
SettingsAuthResponseParser::parse(const DBPoco::Net::HTTPResponse & response, std::istream * body_stream) const
{
    Result result;

    if (response.getStatus() != DBPoco::Net::HTTPResponse::HTTPStatus::HTTP_OK)
        return result;
    result.is_ok = true;

    if (!body_stream)
        return result;

    DBPoco::JSON::Parser parser;
    DBPoco::JSON::Object::Ptr parsed_body;

    try
    {
        DBPoco::Dynamic::Var json = parser.parse(*body_stream);
        const DBPoco::JSON::Object::Ptr & obj = json.extract<DBPoco::JSON::Object::Ptr>();
        DBPoco::JSON::Object::Ptr settings_obj = obj->getObject(settings_key);

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
