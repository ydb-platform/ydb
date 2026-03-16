#pragma once

#include "clickhouse_config.h"

#include <base/types.h>
#include <DBPoco/DOM/Document.h>
#include <DBPoco/DOM/AutoPtr.h>

#if USE_YAML_CPP

namespace DB
{

/// Real YAML parser: loads yaml file into a YAML::Node
class YAMLParserImpl
{
public:
    static DBPoco::AutoPtr<DBPoco::XML::Document> parse(const String& path);
};

using YAMLParser = YAMLParserImpl;

}

#else

namespace DB
{

/// Fake YAML parser: throws an exception if we try to parse YAML configs in a build without yaml-cpp
class DummyYAMLParser
{
public:
    static DBPoco::AutoPtr<DBPoco::XML::Document> parse(const String & path);
};

using YAMLParser = DummyYAMLParser;

}

#endif
