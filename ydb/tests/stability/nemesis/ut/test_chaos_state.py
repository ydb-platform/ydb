from ydb.tests.stability.nemesis.internal.orchestrator.nemesis import chaos_state


def test_store_can_construct_only_the_enabled_planners(monkeypatch):
    built = []

    def build_one(nemesis_type):
        built.append(nemesis_type)
        return object()

    def build_all():
        raise AssertionError("built disabled planners")

    monkeypatch.setattr(chaos_state, "build_planner", build_one)
    monkeypatch.setattr(chaos_state, "build_all_planners", build_all)

    store = chaos_state.ChaosOrchestratorStore(
        planner_types=["KillNodeNemesis"],
    )

    assert built == ["KillNodeNemesis"]
    assert list(store._planners) == ["KillNodeNemesis"]
