#pragma once
#include <map>

namespace DB_CHDB
{

class NamedCollection;
using NamedCollectionPtr = std::shared_ptr<const NamedCollection>;
using MutableNamedCollectionPtr = std::shared_ptr<NamedCollection>;
using NamedCollectionsMap = std::map<std::string, MutableNamedCollectionPtr>;

}
