LIBRARY()

CXXFLAGS(-DMKQL_DISABLE_CODEGEN)

INCLUDE(../ya.make.inc)

PEERDIR(
    yql/essentials/minikql/comp_nodes/no_llvm
    yql/essentials/minikql/computation/no_llvm
    yql/essentials/minikql/codegen/no_llvm
    yql/essentials/minikql/invoke_builtins/no_llvm
)

END()
