LIBRARY() 
 
OWNER( 
    spuchin 
    g:kikimr 
) 
 
SRCS( 
    kqp_opt.cpp
    kqp_opt_build_txs.cpp
    kqp_opt_effects.cpp 
    kqp_opt_join.cpp 
    kqp_opt_kql.cpp 
    kqp_opt_phase.cpp 
    kqp_opt_phy_check.cpp
    kqp_opt_phy_finalize.cpp
) 
 
PEERDIR( 
    ydb/core/kqp/common
    ydb/core/kqp/opt/logical
    ydb/core/kqp/opt/peephole
    ydb/core/kqp/opt/physical
    ydb/library/yql/dq/common
    ydb/library/yql/dq/opt
) 
 
YQL_LAST_ABI_VERSION()

END() 
