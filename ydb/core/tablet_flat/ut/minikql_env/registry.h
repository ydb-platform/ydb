#pragma once

namespace NKikimr::NMiniKQL {
class IFunctionRegistry;
}

namespace NKikimr::NTabletFlatExecutor {

const NMiniKQL::IFunctionRegistry* TestBuiltinFunctionRegistry();

}
