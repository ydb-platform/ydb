#pragma once

namespace NSchemeShardUT_Private {

enum class EInStoreSchemaChange {
    DropTtlColumn,
    DropTtlIndex,
    AddTtlColumn,
};

enum class EInStoreAlterReboot {
    WithoutReboot,
    WithReboot,
};

}
