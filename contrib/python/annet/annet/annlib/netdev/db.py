import collections
import functools


def get_db(prepared):
    allowed = _make_allowed_by_seq(prepared)
    return (
        _build_tree(prepared, allowed),
        functools.reduce(set.union, allowed.values()),  # All sequences
    )


def find_true_sequences(hw_model, tree):
    sequences = set()
    for regexp, meta in tree.items():
        if regexp.search(hw_model):
            sequences.update(meta["sequences"])
            sequences.update(find_true_sequences(hw_model, meta["children"]))
    return sequences


def _build_tree(prepared, allowed_by_seq):
    tree = {}
    for seq, regexp in prepared.items():
        sub = tree
        for sub_seq in _seq_subs(seq):
            regexp = prepared[sub_seq]
            if regexp not in sub:
                sub[regexp] = {
                    "sequences": allowed_by_seq[sub_seq],
                    "children": {},
                }
            sub = sub[regexp]["children"]
    return tree


def _seq_subs(seq):
    for index in range(1, len(seq) + 1):
        yield seq[:index]


def _make_allowed_by_seq(sequences):
    all_variants = collections.Counter()
    variants_by_seq = {}

    for seq in sequences:
        variants = _make_seq_variants(seq)
        all_variants.update(variants)
        variants_by_seq[seq] = variants

    return {
        seq: set(variant for variant in variants if all_variants[variant] <= 1)
        for (seq, variants) in variants_by_seq.items()
    }


def _make_seq_variants(seq):
    return set(seq[left:-right] + (seq[-1],) for left in range(len(seq)) for right in range(1, len(seq[left:]) + 1))
