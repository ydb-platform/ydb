#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_pack.h>
#include <yql/essentials/minikql/computation/mkql_custom_list.h>
#include <yql/essentials/minikql/comp_nodes/mkql_factories.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_program_builder.h>
#include <yql/essentials/minikql/mkql_function_registry.h>

#include <util/datetime/cputimer.h>

using namespace NKikimr;
using namespace NKikimr::NMiniKQL;
using namespace NKikimr::NUdf;

static const size_t LIST_SIZE = 5000000ul;

int main(int, char**) {
    try {
        auto functionRegistry = CreateFunctionRegistry(CreateBuiltinRegistry());

        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        TMemoryUsageInfo memInfo("bench");
        THolderFactory holderFactory(alloc.Ref(), memInfo, functionRegistry.Get());
        TProgramBuilder pgmBuilder(env, *functionRegistry);

        auto listType = pgmBuilder.NewListType(pgmBuilder.NewTupleType({pgmBuilder.NewOptionalType(pgmBuilder.NewDataType(EDataSlot::Uint64)),
                                                                        pgmBuilder.NewDataType(EDataSlot::Uint64)}));
        TUnboxedValueVector listItems;
        for (ui64 i = 0; i < LIST_SIZE; ++i) {
            TUnboxedValueVector tupleItems;
            if (i % 2 == 0) {
                tupleItems.emplace_back();
            } else {
                tupleItems.emplace_back(TUnboxedValuePod(i));
            }
            tupleItems.emplace_back(TUnboxedValuePod(i));

            listItems.emplace_back(holderFactory.VectorAsArray(tupleItems));
        }

        TUnboxedValue list(holderFactory.VectorAsArray(listItems));

        TValuePacker packer(true, listType);

        TSimpleTimer timer;
        TStringBuf packed = packer.Pack(list);
        Cerr << "[pack] Elapsed: " << timer.Get() << "\n";

        timer.Reset();
        auto unpackedList = packer.Unpack(packed, holderFactory);
        Cerr << "[unpack] Elapsed: " << timer.Get() << "\n";

        return 0;
    } catch (...) {
        Cerr << "Error: " << CurrentExceptionMessage() << Endl;
        return 1;
    }
}
