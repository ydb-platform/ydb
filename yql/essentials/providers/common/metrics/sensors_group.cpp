#include "sensors_group.h"

#include <util/generic/singleton.h>


namespace NYql {

struct TSensorsRootGroup {
    TSensorsGroupPtr Value;

    TSensorsRootGroup()
        : Value(new TSensorsGroup)
    {
    }
};

TSensorsGroupPtr GetSensorsRootGroup() {
    return Singleton<TSensorsRootGroup>()->Value;
}

} // namspace NYql
