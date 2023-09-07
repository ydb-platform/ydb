LIBRARY()

SRCS(
    kqp_opt_phy_delete_index.cpp
    kqp_opt_phy_effects.cpp
    kqp_opt_phy_indexes.cpp
    kqp_opt_phy_insert_index.cpp
    kqp_opt_phy_insert.cpp
    kqp_opt_phy_uniq_helper.cpp
    kqp_opt_phy_update_index.cpp
    kqp_opt_phy_update.cpp
    kqp_opt_phy_upsert_index.cpp
)

PEERDIR(
    ydb/core/kqp/common
    ydb/library/yql/dq/common
    ydb/library/yql/dq/opt
)

YQL_LAST_ABI_VERSION()

END()
