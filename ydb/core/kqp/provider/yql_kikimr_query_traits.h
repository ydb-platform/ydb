#pragma once 
 
#include <util/string/builder.h> 
 
namespace NYql { 
struct TExprContext; 
namespace NNodes { 
class TExprBase; 
} // namespace NNodes 
} // namespace NYql 
 
namespace NKikimr { 
namespace NKqp { 
 
struct TQueryTraits { 
    ui32 ReadOnly:1  = 1; 
    ui32 WithJoin:1  = 0; 
    ui32 WithSqlIn:1 = 0; 
    ui32 WithIndex:1 = 0; 
    ui32 WithUdf:1   = 0; 
 
    TString ToString() const { 
        return TStringBuilder() << "{ro: " << ReadOnly << ", join: " << WithJoin << ", sqlIn: " << WithSqlIn 
            << ", index: " << WithIndex << ", udf: " << WithUdf << '}'; 
    } 
}; 
 
TQueryTraits CollectQueryTraits(const NYql::NNodes::TExprBase& program, NYql::TExprContext& ctx); 
 
} // namespace NKqp 
} // namespace NKikimr 
