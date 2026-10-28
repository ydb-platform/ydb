import itertools
import unittest
from unittest.mock import patch

from ydb.apps.bridge_skipper.ydbd_mock.lib import model, validator
from ydb.apps.bridge_skipper.ydbd_mock.lib.model import ClusterState, Stored

D, N1, N2, S, X = (Stored.DISCONNECTED, Stored.NOT_SYNCHRONIZED_1, Stored.NOT_SYNCHRONIZED_2,
                   Stored.SYNCHRONIZED, Stored.SUSPENDED)
PRIMARY, PROMOTED, SYNC, NOTSYNC, SUSP, DISC, UNSPEC = (
    model.PUB_PRIMARY, model.PUB_PROMOTED, model.PUB_SYNCHRONIZED, model.PUB_NOT_SYNCHRONIZED,
    model.PUB_SUSPENDED, model.PUB_DISCONNECTED, model.PUB_UNSPECIFIED)
NAMES2 = ["pile1", "pile2"]
NAMES3 = ["pile1", "pile2", "pile3"]


def st(per_pile, primary=0, promoted=None, generation=1, **kw):
    return ClusterState(per_pile=list(per_pile), primary=primary,
                        promoted=primary if promoted is None else promoted, generation=generation, **kw)


def run(state, updates, names=None, quorum=()):
    return validator.validate_update(state, updates, list(quorum), names or NAMES3[:state.num_piles()])


# CLI shapes (ydb_bridge.cpp)
def failover(pile, new_primary=None):
    out = [(pile, DISC)]
    if new_primary:
        out.append((new_primary, PRIMARY))
    return out


def switchover(new_primary):
    return [(new_primary, PROMOTED)]


def takedown(pile, new_primary=None):
    out = [(pile, SUSP)]
    if new_primary:
        out.append((new_primary, PROMOTED))
    return out


def rejoin(pile):
    return [(pile, NOTSYNC)]


class Case(unittest.TestCase):
    def assertError(self, result, status, message):
        self.assertFalse(result.ok, "expected an error, got %s" % (result.state,))
        self.assertEqual(result.error.status, status)
        self.assertEqual(result.error.message, message)

    def assertState(self, result, per_pile, primary, promoted, generation, sync=None):
        self.assertTrue(result.ok, str(result.error))
        s = result.state
        self.assertEqual([int(x) for x in s.per_pile], [int(x) for x in per_pile])
        self.assertEqual((s.primary, s.promoted, s.generation), (primary, promoted, generation))
        if sync is not None:
            self.assertEqual(s.sync, sync)


class RequestOnlyChecksTest(Case):
    def test_empty_updates(self):
        self.assertError(run(st([S, S]), []), model.BAD_REQUEST, "Updates list cannot be empty")

    def test_duplicate_pile_name(self):
        self.assertError(run(st([S, S]), [("pile2", DISC), ("pile2", SYNC)]),
                         model.BAD_REQUEST, "duplicate update for pile name# pile2")

    def test_two_primaries_in_one_request(self):
        self.assertError(run(st([S, S]), [("pile1", PRIMARY), ("pile2", PRIMARY)]),
                         model.BAD_REQUEST, "multiple primary piles are not allowed in a single request")

    def test_two_promoted_in_one_request(self):
        self.assertError(run(st([S, S, S]), [("pile2", PROMOTED), ("pile3", PROMOTED)]),
                         model.BAD_REQUEST, "multiple promoted piles are not allowed in a single request")

    def test_empty_beats_everything_and_duplicates_beat_unknown_names(self):
        self.assertError(run(st([S, S]), [("zzz", DISC), ("zzz", DISC)]),
                         model.BAD_REQUEST, "duplicate update for pile name# zzz")


class NameAndSuspendedChecksTest(Case):
    def test_unknown_pile(self):
        self.assertError(run(st([S, S]), [("nope", DISC)]), model.BAD_REQUEST, "Invalid pile name# nope")

    def test_unknown_pile_is_checked_before_suspended(self):
        self.assertError(run(st([S, X]), [("pile2", DISC), ("nope", DISC)]),
                         model.BAD_REQUEST, "Invalid pile name# nope")

    def test_update_of_a_suspended_pile_prints_one_based_id(self):
        self.assertError(run(st([S, X]), [("pile2", DISC)]),
                         model.BAD_REQUEST, "cannot update suspended pile: 2")
        self.assertError(run(st([X, S], primary=1), [("pile1", NOTSYNC)]),
                         model.BAD_REQUEST, "cannot update suspended pile: 1")

    def test_suspended_pile_rejects_even_its_current_public_state(self):
        self.assertError(run(st([S, X]), [("pile2", SUSP)]),
                         model.BAD_REQUEST, "cannot update suspended pile: 2")

    def test_suspended_check_comes_before_multiple_primary(self):
        self.assertError(run(st([S, X, S]), [("pile2", DISC), ("pile3", PRIMARY)]),
                         model.BAD_REQUEST, "cannot update suspended pile: 2")

    def test_unnamed_suspended_pile_stays_suspended(self):
        r = run(st([S, S, X]), switchover("pile2"))
        self.assertState(r, [S, S, X], 0, 1, 2)


class DerivationTest(Case):
    def test_second_primary_while_the_old_one_is_unnamed(self):
        self.assertError(run(st([S, S]), [("pile2", PRIMARY)]),
                         model.BAD_REQUEST, "Multiple primary piles are not allowed, found 1 and 2")

    def test_second_promoted_while_a_switchover_is_in_flight(self):
        s = st([S, S, S], primary=0, promoted=2)
        self.assertError(run(s, switchover("pile2")),
                         model.BAD_REQUEST, "Multiple promoted piles are not allowed, found 2 and 3")

    def test_unspecified_state_is_an_internal_error(self):
        self.assertError(run(st([S, S]), [("pile2", UNSPEC)]), model.INTERNAL_ERROR, "Unsupported pile state")

    def test_unknown_future_enum_value_is_an_internal_error(self):
        self.assertError(run(st([S, S]), [("pile2", 77)]), model.INTERNAL_ERROR, "Unsupported pile state")

    def test_known_inactive_enum_value_is_rejected_without_changing_snapshot(self):
        current = st([S, S], generation=4, quorum_piles_last=["pile1"], sync={1: 3})
        snapshot = current.to_json()

        with patch.object(model, "PUB_INACTIVE", 7):
            result = run(current, [("pile2", 7)])

        self.assertError(result, model.BAD_REQUEST,
                         "Pile can't be switched to INACTIVE state, it has to be removed from the cluster instead")
        self.assertEqual(current.to_json(), snapshot)

    def test_unknown_seven_on_old_schema_is_an_internal_error_without_changing_snapshot(self):
        current = st([S, S], generation=4, quorum_piles_last=["pile1"], sync={1: 3})
        snapshot = current.to_json()

        with patch.object(model, "PUB_INACTIVE", None):
            result = run(current, [("pile2", 7)])

        self.assertError(result, model.INTERNAL_ERROR, "Unsupported pile state")
        self.assertEqual(current.to_json(), snapshot)

    def test_no_primary_in_the_result(self):
        self.assertError(run(st([S, S]), [("pile1", DISC)]),
                         model.BAD_REQUEST, "Request must result in a state with one primary pile")

    def test_unsupported_state_beats_missing_primary(self):
        self.assertError(run(st([S, S]), [("pile1", UNSPEC)]), model.INTERNAL_ERROR, "Unsupported pile state")

    def test_primary_never_ends_up_suspended_by_the_mapping(self):
        # Steps 10 are kept for fidelity. Named PRIMARY maps to SYNCHRONIZED, so a request cannot reach them.
        r = run(st([S, S]), [("pile1", SUSP), ("pile2", PRIMARY)])
        self.assertState(r, [X, S], 1, 1, 2, sync={0: 2})


class QuorumPilesTest(Case):
    def test_unknown_quorum_pile(self):
        self.assertError(run(st([S, S]), failover("pile2"), quorum=["nope"]),
                         model.BAD_REQUEST, "Unknown pile name in quorum: nope")

    def test_suspended_quorum_pile(self):
        self.assertError(run(st([S, S]), [("pile2", SUSP)], quorum=["pile2"]),
                         model.BAD_REQUEST, "quorum cannot include suspended pile: pile2")

    def test_quorum_is_stored_and_not_used(self):
        r = run(st([S, S]), failover("pile2"), quorum=["pile1"])
        self.assertState(r, [S, D], 0, 0, 2)
        self.assertEqual(r.state.quorum_piles_last, ["pile1"])

    def test_quorum_check_comes_after_the_primary_check(self):
        self.assertError(run(st([S, S]), [("pile1", DISC)], quorum=["nope"]),
                         model.BAD_REQUEST, "Request must result in a state with one primary pile")


class TransitionRulesTest(Case):
    def test_cannot_switch_to_synchronized_directly(self):
        self.assertError(run(st([S, D]), [("pile2", SYNC)]),
                         model.INTERNAL_ERROR, "Can't switch to SYNCHRONIZED directly")
        self.assertError(run(st([S, N1]), [("pile2", SYNC)]),
                         model.INTERNAL_ERROR, "Can't switch to SYNCHRONIZED directly")
        self.assertError(run(st([S, N2]), [("pile2", SYNC)]),
                         model.INTERNAL_ERROR, "Can't switch to SYNCHRONIZED directly")

    def test_synchronized_to_not_synchronized_is_invalid(self):
        self.assertError(run(st([S, S]), [("pile2", NOTSYNC)]),
                         model.INTERNAL_ERROR, "Invalid transition from SYNCHRONIZED to NOT_SYNCHRONIZED_1")

    def test_first_bad_transition_wins_over_too_many_changes(self):
        s = st([S, D, D])
        r = run(s, [("pile2", SYNC), ("pile3", SYNC)])
        self.assertError(r, model.INTERNAL_ERROR, "Can't switch to SYNCHRONIZED directly")

    def test_multiple_disconnects_are_allowed(self):
        r = run(st([S, S, S]), [("pile2", DISC), ("pile3", DISC)])
        self.assertState(r, [S, D, D], 0, 0, 2)

    def test_multiple_non_disconnect_changes_are_rejected(self):
        r = run(st([S, S, S]), [("pile2", DISC), ("pile3", SUSP)])
        self.assertError(r, model.INTERNAL_ERROR, "Too many state changes in new configuration")

    def test_everything_can_be_disconnected(self):
        for start in (S, N1, N2, X):
            if start == X:
                continue  # a suspended pile can't be updated
            r = run(st([S, start]), failover("pile2"))
            self.assertState(r, [S, D], 0, 0, 2)

    def test_explicit_suspend(self):
        r = run(st([S, S]), takedown("pile2"))
        self.assertState(r, [S, X], 0, 0, 2, sync={1: 2})

    def test_pending_pile_operation_blocks_only_actual_stored_transition(self):
        for kind, verb in (("ADD", "added to"), ("REMOVE", "removed from")):
            state = st([S, D], pending_pile_operation={"kind": kind, "pile_name": "pile2"})
            self.assertError(run(state, rejoin("pile2")), model.INTERNAL_ERROR,
                             "Pile 'pile2' is being %s the cluster, its state can't be changed until BSC finishes" % verb)
            self.assertState(run(state, failover("pile2")), [S, D], 0, 0, 2)


class ReviewDrivenConsequencesTest(Case):
    """Cases that surprised the review. They pin behavior copied from the real server."""

    def test_failover_preserves_an_omitted_resyncing_pile(self):
        s = st([S, N2, S], sync={1: 2})
        self.assertState(run(s, failover("pile3")), [S, N2, D], 0, 0, 2)

    def test_switchover_preserves_an_omitted_resyncing_pile(self):
        s = st([S, S, N2], sync={2: 2}, generation=5)
        r = run(s, switchover("pile2"))
        self.assertState(r, [S, S, N2], 0, 1, 6, sync={2: 2})

    def test_omitted_inactive_pile_is_preserved_but_explicit_update_is_rejected(self):
        s = st([S, S, Stored.INACTIVE])
        self.assertState(run(s, switchover("pile2")), [S, S, Stored.INACTIVE], 0, 1, 2)
        self.assertError(run(s, [("pile3", DISC)]), model.BAD_REQUEST,
                         "pile is not added to the cluster: pile3")

    def test_rejoin_on_n2_moves_it_back_to_n1(self):
        s = st([S, N2], sync={1: 2})
        self.assertState(run(s, rejoin("pile2")), [S, N1], 0, 0, 2, sync={1: 2})

    def test_rejoin_on_n1_is_a_noop_commit(self):
        s = st([S, N1], sync={1: 2}, generation=4)
        self.assertState(run(s, rejoin("pile2")), [S, N1], 0, 0, 5, sync={1: 2})

    def test_rejoin_on_disconnected(self):
        s = st([S, D], sync={1: 2})
        self.assertState(run(s, rejoin("pile2")), [S, N1], 0, 0, 2, sync={1: 2})

    def test_noop_failover_still_bumps_the_generation(self):
        s = st([S, D], sync={1: 2}, generation=7)
        self.assertState(run(s, failover("pile2")), [S, D], 0, 0, 8, sync={1: 2})

    def test_failover_of_primary_during_an_inflight_switchover(self):
        # switchover to pile3 is in flight: pile3 stays PROMOTED.
        s = st([S, S, S], primary=0, promoted=2)
        r = run(s, failover("pile1", "pile2"))
        self.assertState(r, [D, S, S], 1, 2, 2, sync={0: 2})

    def test_takedown_of_the_primary_with_a_new_primary_is_rejected(self):
        self.assertError(run(st([S, S]), takedown("pile1", "pile2")),
                         model.BAD_REQUEST, "Request must result in a state with one primary pile")

    def test_takedown_of_the_primary_alone_is_rejected(self):
        self.assertError(run(st([S, S]), takedown("pile1")),
                         model.BAD_REQUEST, "Request must result in a state with one primary pile")

    def test_switchover_to_the_current_primary_is_rejected(self):
        self.assertError(run(st([S, S]), switchover("pile1")),
                         model.BAD_REQUEST, "Request must result in a state with one primary pile")

    def test_failover_of_the_primary_without_new_primary_is_rejected(self):
        self.assertError(run(st([S, S]), failover("pile1")),
                         model.BAD_REQUEST, "Request must result in a state with one primary pile")


class CliShapesTest(Case):
    def test_failover_primary_to_new_primary(self):
        r = run(st([S, S]), failover("pile1", "pile2"))
        self.assertState(r, [D, S], 1, 1, 2, sync={0: 2})

    def test_failover_follower(self):
        r = run(st([S, S]), failover("pile2"))
        self.assertState(r, [S, D], 0, 0, 2, sync={1: 2})

    def test_failover_follower_with_new_primary_naming_the_current_primary(self):
        # Naming the current primary as PRIMARY again is accepted and changes nothing but pile2.
        r = run(st([S, S]), failover("pile2", "pile1"))
        self.assertState(r, [S, D], 0, 0, 2)

    def test_switchover(self):
        r = run(st([S, S]), switchover("pile2"))
        self.assertState(r, [S, S], 0, 1, 2, sync={})

    def test_switchover_then_promote_commit(self):
        first = run(st([S, S]), switchover("pile2")).state
        self.assertEqual(model.public_states(first), [PRIMARY, PROMOTED])

    def test_takedown_follower(self):
        r = run(st([S, S]), takedown("pile2"))
        self.assertState(r, [S, X], 0, 0, 2, sync={1: 2})

    def test_takedown_follower_with_unneeded_new_primary_naming_a_follower_fails(self):
        # pile3 PROMOTED while pile1 stays primary: a switchover to pile3 plus a suspend of pile2.
        r = run(st([S, S, S]), takedown("pile2", "pile3"))
        self.assertState(r, [S, X, S], 0, 2, 2, sync={1: 2})

    def test_rejoin_from_disconnected_then_connect_style_states(self):
        r = run(st([S, D], sync={1: 2}), rejoin("pile2"))
        self.assertEqual(model.public_states(r.state), [PRIMARY, NOTSYNC])

    def test_update_command_with_explicit_states(self):
        r = run(st([S, S, S]), [("pile1", PRIMARY), ("pile2", SYNC), ("pile3", DISC)])
        self.assertState(r, [S, S, D], 0, 0, 2, sync={2: 2})

    def test_update_that_names_everything(self):
        r = run(st([S, S]), [("pile1", DISC), ("pile2", PRIMARY)])
        self.assertState(r, [D, S], 1, 1, 2)


class ExhaustiveInvariantsTest(Case):
    """Every CLI shape from every reachable starting vector on 2 and 3 piles."""

    COMMANDS = (failover, switchover, takedown, rejoin)

    def starts(self, n):
        for combo in itertools.product(list(Stored), repeat=n):
            for primary in range(n):
                if combo[primary] != S:
                    continue
                for promoted in range(n):
                    if combo[promoted] != S:
                        continue
                    sync = {i: 2 for i, x in enumerate(combo) if x != S}
                    yield st(combo, primary=primary, promoted=promoted, generation=3, sync=sync)

    def requests(self, names):
        for pile in names:
            yield failover(pile)
            yield takedown(pile)
            yield rejoin(pile)
            for other in names:
                if other != pile:
                    yield failover(pile, other)
                    yield takedown(pile, other)
        for other in names:
            yield switchover(other)

    def check(self, names):
        count_ok = count_err = 0
        for start in self.starts(len(names)):
            before = start.to_json()
            for updates in self.requests(names):
                r = validator.validate_update(start, updates, [], names)
                self.assertEqual(start.to_json(), before, "validator mutated its input")
                if not r.ok:
                    count_err += 1
                    self.assertIn(r.error.status, (model.BAD_REQUEST, model.INTERNAL_ERROR))
                    self.assertTrue(r.error.message)
                    continue
                count_ok += 1
                new = r.state
                self.assertEqual(new.generation, start.generation + 1)
                self.assertLessEqual(len(model.changed_piles(start, new)), 1, (start, updates))
                self.assertEqual(new.per_pile[new.primary], S)
                self.assertEqual(new.per_pile[new.promoted], S)
                publics = model.public_states(new)
                self.assertEqual(publics.count(PRIMARY), 1)
                self.assertLessEqual(publics.count(PROMOTED), 1)
                for i in model.changed_piles(start, new):
                    self.assertIn(new.per_pile[i], (D, X, N1))
                    if new.per_pile[i] == N1:
                        self.assertNotEqual(start.per_pile[i], S)
                for i in range(len(names)):
                    if new.per_pile[i] == S:
                        self.assertNotIn(i, new.sync)
                    if start.per_pile[i] == X and new.per_pile[i] != X:
                        self.fail("a suspended pile was updated")
        self.assertGreater(count_ok, 0)
        self.assertGreater(count_err, 0)

    def test_two_piles(self):
        self.check(NAMES2)

    def test_three_piles(self):
        self.check(NAMES3)


if __name__ == "__main__":
    unittest.main()
