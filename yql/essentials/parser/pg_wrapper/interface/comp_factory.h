#pragma once

#include <functional>

namespace NKikimr {
namespace NMiniKQL {

class IComputationNode;
class TCallable;
struct TComputationNodeFactoryContext;

} // namespace NMiniKQL
} // namespace NKikimr

namespace NYql {

std::function<NKikimr::NMiniKQL::IComputationNode*(NKikimr::NMiniKQL::TCallable&,
                                                   const NKikimr::NMiniKQL::TComputationNodeFactoryContext&)>
GetPgFactory();

} // namespace NYql
