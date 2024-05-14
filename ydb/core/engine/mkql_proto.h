#pragma once
#include <ydb/library/yql/minikql/defs.h>
#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/mkql_proto/protos/minikql.pb.h>
#include <ydb/public/api/protos/ydb_value.pb.h>
#include <ydb/library/mkql_proto/mkql_proto.h>
#include <ydb/core/scheme/scheme_tablecell.h>

namespace NKikimr {

namespace NMiniKQL {

class THolderFactory;

// NOTE: TCell's can reference memomry from tupleValue
bool CellsFromTuple(const NKikimrMiniKQL::TType* tupleType,
                    const NKikimrMiniKQL::TValue& tupleValue,
                    const TConstArrayRef<NScheme::TTypeInfo>& expectedTypes,
                    TVector<bool> notNullTypes,
                    bool allowCastFromString,
                    TVector<TCell>& key,
                    TString& errStr,
                    TVector<TString>& memoryOwner);

bool CellToValue(NScheme::TTypeInfo type, const TCell& c, NKikimrMiniKQL::TValue& val, TString& errStr);

} // namspace NMiniKQL
} // namspace NKikimr
