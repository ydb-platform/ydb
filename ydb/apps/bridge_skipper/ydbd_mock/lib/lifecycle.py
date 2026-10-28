"""Public lifecycle transitions and the test lab's deployed-config prerequisite model.

Physical processes are provisioned by Topology; registry slots are independent of
their inventory indexes. Dropped slots are reused and trailing holes are trimmed.
"""
from ydb.apps.bridge_skipper.ydbd_mock.lib import model
from ydb.apps.bridge_skipper.ydbd_mock.lib.model import MockError, Stored


def configured(state, names):
    return list(names) if state.configured_piles is None else list(state.configured_piles)


def configure(current, values, names, initial=False):
    unknown = set(values) - {"configured_piles", "bridged", "self_management"}
    if unknown:
        raise ValueError("unknown topology prerequisites: %s" % sorted(unknown))
    wanted = values.get("configured_piles", configured(current, names))
    if not isinstance(wanted, list) or not all(isinstance(n, str) for n in wanted):
        raise ValueError("configured_piles must be a list of inventory pile names")
    if not wanted or len(wanted) != len(set(wanted)) or set(wanted) - set(names):
        raise ValueError("configured_piles must be nonempty, unique and drawn from inventory")
    for key in ("bridged", "self_management"):
        if key in values and not isinstance(values[key], bool):
            raise ValueError("%s must be a boolean" % key)
    baseline = initial or values.get("bridged") is False
    if not baseline and "bridged" in values and values["bridged"] != current.bridged:
        raise ValueError("use ConvertToBridge to enable bridge mode")
    if current.pending_pile_operation and not baseline:
        raise ValueError("cannot change configuration during a pile operation")
    old_names = current.registered_names(names)
    for name in set(old_names) - set(wanted):
        if not baseline and current.stored(names.index(name)) != Stored.INACTIVE:
            raise ValueError("pile '%s' must be INACTIVE before dropping its configuration" % name)
    new = model.derive_metadata(current)
    new.configured_piles = list(wanted)
    if baseline:
        new.registry = list(wanted)
        new.per_pile = [Stored.SYNCHRONIZED if name in wanted else Stored.INACTIVE for name in names]
        new.primary = new.promoted = names.index(wanted[0])
        new.sync = {}
        new.conversion_in_progress = False
        new.pending_pile_operation = None
    else:
        registry = list(current.registry) if current.registry is not None else list(names)
        registry = [name if name in wanted else None for name in registry]
        for name in wanted:
            if name not in registry:
                if None in registry:
                    registry[registry.index(None)] = name
                else:
                    registry.append(name)
                new.per_pile[names.index(name)] = Stored.INACTIVE
        while registry and registry[-1] is None:
            registry.pop()
        new.registry = registry
    new.bridged = values.get("bridged", current.bridged)
    new.self_management = values.get("self_management", current.self_management)
    old_slots = current.registry if current.registry is not None else names
    before = [Stored.INACTIVE if name is None else current.stored(names.index(name)) for name in old_slots]
    after = [Stored.INACTIVE if name is None else new.stored(names.index(name)) for name in new.registry]
    if current.bridged and not baseline and before != after:
        new.generation += 1
    return new


def start(current, kind, name, names):
    def fail(message):
        return MockError(model.INTERNAL_ERROR, message)

    if kind == "convert":
        if not current.self_management:
            return fail("Self-management must be enabled to convert cluster to bridged one")
        if len(configured(current, names)) != 1:
            return fail("BridgeConfig must contain exactly one pile to convert cluster to bridged one")
        if current.bridged:
            return fail("Cluster is already bridged one")
        new = model.derive_metadata(current)
        new.bridged = True
        new.conversion_in_progress = True
        return new
    if not current.bridged:
        return fail("Bridge mode is not enabled")
    verb = "added to" if kind == "add" else "removed from"
    if not current.self_management:
        return fail("Self-management must be enabled for piles to be %s the cluster" % verb)
    if name not in current.registered_names(names):
        return fail("Pile '%s' is not listed in BridgeConfig" % name)
    if current.pending_pile_operation:
        return fail("Operation on pile '%s' is in progress" % current.pending_pile_operation["pile_name"])
    index = names.index(name)
    expected = Stored.INACTIVE if kind == "add" else Stored.DISCONNECTED
    if current.stored(index) != expected:
        return fail("Pile '%s' has to be %s to be %s the cluster, its state is %s" % (
            name, expected.name, verb, current.stored(index).name))
    if current.conversion_in_progress:
        return fail("Piles can't be %s the cluster until BSC finishes conversion of the cluster" % verb)
    new = (model.derive(current, updates={index: Stored.DISCONNECTED}) if kind == "add"
           else model.derive_metadata(current))
    new.pending_pile_operation = {
        "pile_name": name, "pile_index": index, "kind": kind.upper(), "error_reason": "",
        "operation_id": new.revision,
    }
    return new
