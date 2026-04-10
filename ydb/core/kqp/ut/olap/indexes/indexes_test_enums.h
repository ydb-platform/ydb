#pragma once

namespace NKikimr {
namespace NKqp {

enum EUseQueryService {
    SchemeQuery,
    QueryService
};

enum ELocalIndexAsSchemeObject {
    SchemeObjectDisabled,
    SchemeObjectEnabled
};

} // namespace NKqp
} // namespace NKikimr
