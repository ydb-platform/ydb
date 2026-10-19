YQL_LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yql/essentials/public/purecalc/common/ya.make.inc)

PEERDIR(
    yql/essentials/minikql/invoke_builtins  # mkql_builtins.h of purecalc/common/program_factory.h: the slot interface
)

END()
