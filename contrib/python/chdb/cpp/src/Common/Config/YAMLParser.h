#pragma once

#include "clickhouse_config.h"

#include <Common/ErrorCodes.h>
#include <Common/Exception.h>
#include <base/types.h>
#include <CHDBPoco/DOM/Document.h>
#include <CHDBPoco/DOM/AutoPtr.h>

#if USE_YAML_CPP

namespace DB_CHDB
{

/// Real YAML parser: loads yaml file into a YAML::Node
class YAMLParserImpl
{
public:
    static CHDBPoco::AutoPtr<CHDBPoco::XML::Document> parse(const String& path);
};

using YAMLParser = YAMLParserImpl;

}

#else

namespace DB_CHDB
{

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_YAML;
}

/// Fake YAML parser: throws an exception if we try to parse YAML configs in a build without yaml-cpp
class DummyYAMLParser
{
public:
    static CHDBPoco::AutoPtr<CHDBPoco::XML::Document> parse(const String& path)
    {
        CHDBPoco::AutoPtr<CHDBPoco::XML::Document> xml = new CHDBPoco::XML::Document;
        throw Exception(ErrorCodes::CANNOT_PARSE_YAML, "Unable to parse YAML configuration file {} without usage of yaml-cpp library", path);
        return xml;
    }
};

using YAMLParser = DummyYAMLParser;

}

#endif
