import itertools
import math
import unittest

from ydb.core.kqp.opt.rbo.verification.rbo_verifier.string_order import (
    MAX_REPRESENTATIVES,
    MAX_REPRESENTATIVE_BYTES,
    MAX_STRING_BYTES,
    StringOrderUniverse,
)


def _compare(left, right):
    return _compare_bytes(left.encode("utf-8"), right.encode("utf-8"))


def _compare_bytes(left, right):
    return (left > right) - (left < right)


def _signature(values, literals):
    result = tuple(
        _compare(value, literal)
        for value in values
        for literal in literals
    )
    return result + tuple(
        _compare(values[left], values[right])
        for left in range(len(values))
        for right in range(left)
    )


def _words(alphabet, max_length):
    return tuple(
        b"".join(word).decode("utf-8")
        for length in range(max_length + 1)
        for word in itertools.product(alphabet, repeat=length)
    )


class StringOrderUniverseTest(unittest.TestCase):
    def test_no_literals_uses_one_nul_prefix_chain(self):
        universe = StringOrderUniverse([], 4)

        self.assertEqual(universe.representatives, ("", "\0", "\0\0", "\0\0\0"))
        self.assertEqual(
            universe.encoded_representatives,
            (b"", b"\0", b"\0\0", b"\0\0\0"),
        )

    def test_zero_nonliteral_terms_needs_only_literals(self):
        universe = StringOrderUniverse(["z", "", "a"], 0)

        self.assertEqual(universe.representatives, ("", "a", "z"))

    def test_empty_literal_is_distinct_from_no_literals(self):
        universe = StringOrderUniverse([""], 2)

        self.assertEqual(universe.representatives, ("", "\0", "\0\0"))
        self.assertEqual(universe.rank(""), 0)

    def test_finite_interval_below_nul_prefix_is_not_overpopulated(self):
        universe = StringOrderUniverse(["\0\0\0"], 5)

        below = tuple(
            value
            for value in universe.representatives
            if value.encode("utf-8") < b"\0\0\0"
        )
        self.assertEqual(
            below,
            ("", "\0", "\0\0"),
        )

    def test_finite_interval_between_prefix_and_nuls_is_not_overpopulated(self):
        universe = StringOrderUniverse(["a", "a\0\0\0", "b"], 5)

        between = tuple(
            value
            for value in universe.representatives
            if b"a" < value.encode("utf-8") < b"a\0\0\0"
        )
        self.assertEqual(between, ("a\0", "a\0\0"))

    def test_finite_intervals_are_capped_by_the_term_bound(self):
        below = StringOrderUniverse(["\0\0\0\0"], 2)
        between = StringOrderUniverse(["a", "a\0\0\0\0"], 2)

        self.assertEqual(below.representatives[:2], ("", "\0"))
        self.assertEqual(
            tuple(
                value
                for value in between.representatives
                if b"a" < value.encode("utf-8") < b"a\0\0\0\0"
            ),
            ("a\0", "a\0\0"),
        )

    def test_infinite_intervals_have_the_full_term_bound(self):
        universe = StringOrderUniverse(["\0\0a", "a", "a\0\0x"], 3)
        encoded = universe.encoded_representatives

        intervals = (
            (None, b"\0\0a"),
            (b"\0\0a", b"a"),
            (b"a", b"a\0\0x"),
            (b"a\0\0x", None),
        )
        for lower, upper in intervals:
            with self.subTest(lower=lower, upper=upper):
                values = tuple(
                    value
                    for value in encoded
                    if (lower is None or lower < value)
                    and (upper is None or value < upper)
                )
                self.assertEqual(len(values), 3)

    def test_literals_are_raw_utf8_ordered_without_normalization(self):
        literals = ("😀", "é", "z", "e\u0301", "\x7f", "\u0080")
        universe = StringOrderUniverse(reversed(literals), 1)
        ordered = tuple(sorted(literals, key=lambda value: value.encode("utf-8")))

        self.assertEqual(
            tuple(sorted(literals, key=universe.rank)),
            ordered,
        )
        self.assertLess(universe.rank("e\u0301"), universe.rank("é"))
        self.assertNotEqual(universe.rank("e\u0301"), universe.rank("é"))

    def test_invalid_string_bytes_have_replayable_order_representatives(self):
        literals = ("\x7f", "\u0080")
        universe = StringOrderUniverse(literals, 3)
        lower, upper = (value.encode("utf-8") for value in literals)
        representatives = tuple(
            value
            for value in universe.encoded_representatives
            if lower < value < upper
        )
        arbitrary_string_values = (b"\x80", b"\xc0", b"\xc1\xff")

        def signature(values):
            return tuple(
                _compare_bytes(value, literal)
                for value in values
                for literal in (lower, upper)
            ) + tuple(
                _compare_bytes(values[left_index], values[right_index])
                for left_index in range(len(values))
                for right_index in range(left_index)
            )

        self.assertEqual(len(representatives), 3)
        self.assertEqual(
            signature(arbitrary_string_values),
            signature(representatives),
        )
        self.assertTrue(
            all(value.decode("utf-8").encode("utf-8") == value for value in representatives)
        )

    def test_construction_is_deterministic_and_deduplicates_literals(self):
        literals = ("z", "", "a", "\0", "é")
        expected = StringOrderUniverse(literals, 2)

        for permutation in itertools.permutations(literals):
            self.assertEqual(
                StringOrderUniverse(permutation + ("a",), 2),
                expected,
            )

    def test_rank_and_decode_are_exact_inverses(self):
        universe = StringOrderUniverse(["z", "a", "é"], 2)

        for rank, value in enumerate(universe.representatives):
            self.assertEqual(universe.rank(value), rank)
            self.assertEqual(universe.representative(rank), value)

        for invalid_rank in (-1, len(universe), True, 1.0):
            with self.subTest(rank=invalid_rank):
                with self.assertRaises(ValueError):
                    universe.representative(invalid_rank)
        with self.assertRaises(ValueError):
            universe.rank("not present")

    def test_invalid_counts_and_inputs_fail_closed(self):
        for count in (-1, True, 1.0, "1"):
            with self.subTest(count=count):
                with self.assertRaises(ValueError):
                    StringOrderUniverse([], count)

        for literals in ("one string", b"bytes", None):
            with self.subTest(literals=literals):
                with self.assertRaises(ValueError):
                    StringOrderUniverse(literals, 1)

        for literal in (b"bytes", 1, None, "\ud800", "x\udfff"):
            with self.subTest(literal=literal):
                with self.assertRaises(ValueError):
                    StringOrderUniverse([literal], 1)

    def test_preflight_rejects_rank_and_byte_budgets_before_allocation(self):
        with self.assertRaisesRegex(ValueError, r"requires \d+ ranks; limit is"):
            StringOrderUniverse([], MAX_REPRESENTATIVES + 1)

        quadratic_count = math.isqrt(2 * MAX_REPRESENTATIVE_BYTES) + 2
        self.assertLess(quadratic_count, MAX_REPRESENTATIVES)
        with self.assertRaisesRegex(
            ValueError,
            r"requires \d+ encoded bytes; limit is",
        ):
            StringOrderUniverse([], quadratic_count)

    def test_preflight_shares_the_replay_cell_size_budget(self):
        boundary = "x" * MAX_STRING_BYTES
        self.assertEqual(
            StringOrderUniverse([boundary], 0).representatives,
            (boundary,),
        )

        with self.assertRaisesRegex(
            ValueError,
            rf"value of {MAX_STRING_BYTES + 1} encoded bytes; limit is",
        ):
            StringOrderUniverse([boundary + "x"], 0)

        with self.assertRaisesRegex(
            ValueError,
            rf"value of {MAX_STRING_BYTES + 1} encoded bytes; limit is",
        ):
            StringOrderUniverse([boundary], 1)

    def test_every_representative_is_valid_utf8_and_strictly_byte_ordered(self):
        universe = StringOrderUniverse(
            ["", "\0\0", "e\u0301", "é", "😀"],
            4,
        )
        encoded = universe.encoded_representatives

        self.assertTrue(
            all(value.decode("utf-8").encode("utf-8") == value for value in encoded)
        )
        self.assertTrue(
            all(left < right for left, right in zip(encoded, encoded[1:]))
        )

    def test_exhaustive_small_byte_alphabet_has_exact_order_signatures(self):
        concrete_domain = _words((b"\0", b"\1"), max_length=2)

        for literal_mask in range(1 << len(concrete_domain)):
            literals = tuple(
                value
                for index, value in enumerate(concrete_domain)
                if literal_mask & (1 << index)
            )
            ordered_literals = tuple(
                sorted(literals, key=lambda value: value.encode("utf-8"))
            )
            for max_terms in range(3):
                universe = StringOrderUniverse(literals, max_terms)
                sample_domain = tuple(
                    dict.fromkeys(concrete_domain + universe.representatives)
                )
                for term_count in range(max_terms + 1):
                    with self.subTest(
                        literals=literals,
                        max_terms=max_terms,
                        term_count=term_count,
                    ):
                        representative_signatures = {
                            _signature(values, ordered_literals)
                            for values in itertools.product(
                                universe.representatives,
                                repeat=term_count,
                            )
                        }
                        concrete_signatures = {
                            _signature(values, ordered_literals)
                            for values in itertools.product(
                                sample_domain,
                                repeat=term_count,
                            )
                        }
                        self.assertEqual(
                            concrete_signatures,
                            representative_signatures,
                        )


if __name__ == "__main__":
    unittest.main()
