#include <Disks/ObjectStorages/StoredObject.h>

#include <vector>

namespace DB_CHDB
{

size_t getTotalSize(const StoredObjects & objects)
{
    size_t size = 0;
    for (const auto & object : objects)
        size += object.bytes_size;
    return size;
}

}
