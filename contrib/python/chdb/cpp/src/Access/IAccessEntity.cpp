#include <Access/IAccessEntity.h>


namespace DB_CHDB
{

bool IAccessEntity::equal(const IAccessEntity & other) const
{
    return (name == other.name) && (getType() == other.getType());
}

}
