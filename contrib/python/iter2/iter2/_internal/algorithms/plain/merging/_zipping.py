from dataclasses import dataclass

from ....lib.std import (
    builtin_zip,
    itertools_zip_longest,
    itertools_starmap,
)

from ....lib.undefined import UNDEFINED


# ---

zip_shortest = builtin_zip


def zip_same_size(*iterables):
    return zip_shortest(*iterables, strict=True)


# ---

def zip_longest(*iterables, fill_value=UNDEFINED, fill_values=UNDEFINED):
    if UNDEFINED.is_not(fill_values):
        return _zip_longest_with_separate_fill_values(iterables, fill_values)
    else:
        if not UNDEFINED.is_not(fill_value):
            fill_value = None
        return itertools_zip_longest(*iterables, fillvalue=fill_value)


@dataclass(slots=True)
class _ZipContext:
    non_finished_iterables_mask: int = 0  # 2^iterables_count - 1

    def iterate_with_fill_value(self, iterable, fill_value):
        # 1. Register iterable as non-finished
        self_mask = self.non_finished_iterables_mask + 1
        self.non_finished_iterables_mask |= self_mask

        # Separation of registration and generator initialization is critical!
        return self._iterate_with_fill_value(iterable, fill_value, self_mask)

    def _iterate_with_fill_value(self, iterable, fill_value, self_mask):
        # 2. Emit items from original iterable
        yield from iterable

        # 3. Unregister iterable in non-finished
        self.non_finished_iterables_mask ^= self_mask

        # 4. Emit fill value while non-finished iterables exist
        while self.non_finished_iterables_mask > 0:
            yield fill_value


def _zip_longest_with_separate_fill_values(iterables, fill_values):
    return zip_shortest(*itertools_starmap(
        _ZipContext().iterate_with_fill_value,
        zip_shortest(iterables, fill_values),
    ))
