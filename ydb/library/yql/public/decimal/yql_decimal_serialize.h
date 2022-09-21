#pragma once

#include <ydb/library/yql/public/decimal/yql_decimal.h>

namespace NYql {
namespace NDecimal {

// big-endian 16 bytes buffer.
size_t Serialize(TInt128 v, char* buff);
std::pair<TInt128, size_t> Deserialize(const char* buff, size_t len);

}
}
