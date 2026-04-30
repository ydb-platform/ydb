PY3_PROGRAM()

PY_SRCS(
    __main__.py
    internal/config.py
    internal/agent/agent_warden_catalog.py
    internal/orchestrator/orchestrator_warden_catalog.py
    internal/models.py
    internal/safety_warden_execution.py
    internal/event_loop.py
    internal/nemesis/catalog.py
    internal/nemesis/local_network.py
    internal/nemesis/monitored_actor.py
    internal/nemesis/cluster_context.py
    internal/nemesis/cluster_entries.py
    internal/nemesis/runners/__init__.py
    internal/nemesis/runners/network.py
    internal/nemesis/runners/node_local.py
    internal/nemesis/runners/cluster_disk.py
    internal/nemesis/runners/cluster_hive.py
    internal/nemesis/runners/cluster_node.py
    internal/nemesis/runners/cluster_host.py
    internal/nemesis/runners/cluster_tablets.py
    internal/nemesis/runners/datacenter.py
    internal/nemesis/runners/bridge_pile.py
    internal/nemesis/runners/yaml_gates.py
    internal/nemesis/chaos_dispatch.py
    internal/agent/agent_warden_checker.py
    internal/agent/nemesis/runner.py
    internal/orchestrator/install.py
    internal/orchestrator/orchestrator_warden_execution.py
    internal/orchestrator/unified_agent_verify_failed_aggregated.py
    internal/orchestrator/orchestrator_warden_checker.py
    internal/orchestrator/nemesis/schedule_loop.py
    internal/orchestrator/nemesis/chaos_state.py
    internal/orchestrator/nemesis/default_planner.py
    internal/orchestrator/nemesis/pinned_first_host_planner.py
    internal/orchestrator/nemesis/serial_staggered_planner.py
    internal/orchestrator/nemesis/topology_fanout_planner.py
    internal/orchestrator/nemesis/network_planner.py
    internal/orchestrator/nemesis/nemesis_planner_base.py
    routers/agent_router.py
    routers/orchestrator_router.py
    app.py
)

RESOURCE(
    static/index.html             nemesis_static/index.html
    static/HostProcessItem.js     nemesis_static/HostProcessItem.js
    static/HostStatusItem.js      nemesis_static/HostStatusItem.js
    static/NemesisGroupAccordion.js nemesis_static/NemesisGroupAccordion.js
    static/ProcessCard.js         nemesis_static/ProcessCard.js
    static/ProcessTypeGroup.js    nemesis_static/ProcessTypeGroup.js
    static/WardenChecksCard.js    nemesis_static/WardenChecksCard.js
)

DEPENDS(
    ydb/tools/cfg/bin
    ydb/tests/tools/nemesis/driver
)

PEERDIR(
    ydb/tests/tools/nemesis/library
    ydb/tests/library
    ydb/tests/library/stability
    ydb/tests/library/wardens
    contrib/python/Flask
    library/python/resource
)

END()
