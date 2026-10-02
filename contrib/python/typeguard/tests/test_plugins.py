from pytest import MonkeyPatch

from typeguard import load_plugins


def test_custom_type_checker(monkeypatch: MonkeyPatch) -> None:
    def lookup_func(origin_type, args, extras):
        pass

    class FakeEntryPoint:
        name = "test"

        def load(self):
            return lookup_func

    def fake_entry_points(group):
        assert group == "typeguard.checker_lookup"
        return [FakeEntryPoint()]

    checker_lookup_functions = []
    monkeypatch.setattr("typeguard._checkers.entry_points", fake_entry_points)
    monkeypatch.setattr(
        "typeguard._checkers.checker_lookup_functions", checker_lookup_functions
    )
    load_plugins()
    assert checker_lookup_functions[0] is lookup_func
