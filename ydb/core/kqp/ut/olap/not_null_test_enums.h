#pragma once

namespace NKikimr::NKqp::NSetNotNull {

enum class ETableKind {
    Row,
    Column,
};

enum class EFeatureFlag {
    Disabled,
    Enabled,
};

enum class EReboot {
    No,
    Yes,
};

} // namespace NKikimr::NKqp::NSetNotNull
