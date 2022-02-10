#include "converter.h"

#include <ydb/library/mkql_proto/protos/minikql.pb.h>


namespace NKikimr {
namespace NResultLib {

TStruct ConvertResult(const NKikimrMiniKQL::TValue& value, const NKikimrMiniKQL::TType& type) {
    return TStruct(value, type);
}

} // namespace NResultLib
} // namespace NKikimr
