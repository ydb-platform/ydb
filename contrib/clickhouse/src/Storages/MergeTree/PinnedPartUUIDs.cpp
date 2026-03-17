#include "PinnedPartUUIDs.h"
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <DBPoco/JSON/JSON.h>
#include <DBPoco/JSON/Object.h>
#include <DBPoco/JSON/Parser.h>

namespace DB
{

String PinnedPartUUIDs::toString() const
{
    std::vector<UUID> vec(part_uuids.begin(), part_uuids.end());

    DBPoco::JSON::Object json;
    json.set(JSON_KEY_UUIDS, DB::toString(vec));

    std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    oss.exceptions(std::ios::failbit);
    json.stringify(oss);

    return oss.str();
}

void PinnedPartUUIDs::fromString(const String & buf)
{
    DBPoco::JSON::Parser parser;
    auto json = parser.parse(buf).extract<DBPoco::JSON::Object::Ptr>();

    std::vector<UUID> vec = parseFromString<std::vector<UUID>>(json->getValue<std::string>(PinnedPartUUIDs::JSON_KEY_UUIDS));

    part_uuids.clear();
    std::copy(vec.begin(), vec.end(), std::inserter(part_uuids, part_uuids.begin()));
}

}
