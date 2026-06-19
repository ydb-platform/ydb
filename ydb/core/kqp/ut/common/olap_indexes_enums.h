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

enum EOlapTableType {
    Table,
    TableStore
};

} // namespace NKqp
} // namespace NKikimr
