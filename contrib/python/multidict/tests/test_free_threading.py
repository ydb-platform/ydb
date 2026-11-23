import threading
import traceback

import pytest

from multidict import CIMultiDict, MultiDict, MutableMultiMapping


@pytest.mark.c_extension
def test_race_condition_iterator_vs_mutation(
    any_multidict_class: type[CIMultiDict[str] | MultiDict[str]],
) -> None:
    """Test that concurrent iterations and mutations do not cause a memory safety violation.

    This test specifically triggers use-after-free scenarios if the underlying C extension
    hash table ``md->keys`` resizes concurrently during an unresolved iteration sequence.
    Under free-threaded CPython (GIL disabled), this previously resulted in a SIGSEGV.

    With the issue fixed, the code securely catches size mutations and cleanly raises
    a standard Python ``RuntimeError`` ('MultiDict is changed during iteration'), preventing
    crashes.
    """
    if getattr(any_multidict_class, "__module__", "").endswith("_multidict_py"):
        pytest.skip("Test is only applicable to the C extension")

    md: MutableMultiMapping[str] = any_multidict_class()
    for i in range(8):
        md[f"init-{i}"] = f"v{i}"

    errors: list[tuple[str, int, str, str, str]] = []

    def writer(target: MutableMultiMapping[str]) -> None:
        for i in range(256):
            try:
                target[f"k-{i % 64}"] = f"v{i}"
                # add() and popone() reach md_add() and md_pop_one(), whose
                # entry points used to run ASSERT_CONSISTENT() outside their
                # critical section.
                target.add(f"k-{i % 64}", f"a{i}")
                target.popone(f"k-{i % 64}", None)
                target.setdefault(f"k-{i % 64}", f"d{i}")
            except RuntimeError:  # pragma: no cover
                # "MultiDict changed during iteration" is expected under contention
                pass
            except Exception as e:  # pragma: no cover
                errors.append(
                    ("writer", i, type(e).__name__, str(e), traceback.format_exc())
                )

    def reader(target: MutableMultiMapping[str]) -> None:
        for i in range(256):
            try:
                list(target.items())
                list(target.keys())
                list(target.values())
                # getall()/get() walk the table with a finder open, which is
                # what makes an unlocked consistency check observable.
                target.getall(f"k-{i % 64}", None)
                target.get(f"k-{i % 64}", None)
            except RuntimeError:
                # "MultiDict changed during iteration" is exactly the expected
                # and memory-safe outcome when iterating a resizing dictionary.
                pass
            except Exception as e:  # pragma: no cover
                errors.append(("reader", i, type(e).__name__, str(e), ""))

    threads = [
        threading.Thread(target=f, args=(md,)) for f in [writer, reader, writer, reader]
    ]

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # The test passes if it survives without a segmentation fault (SIGSEGV/SIGABRT).
    # If the C-extension is thread-safe, no Python exceptions other than RuntimeError
    # (handled above) should inadvertently surface to the user.
    assert not errors, f"Unexpected errors during concurrent execution: {errors}"


@pytest.mark.c_extension
def test_race_condition_extend_vs_source_mutation(
    any_multidict_class: type[CIMultiDict[str] | MultiDict[str]],
) -> None:
    """Test that reading a second multidict is safe while that one mutates.

    ``md_update_from_ht()`` takes a raw ``entry_t *`` into the *source*
    multidict's table and walks it. The destination is locked by the calling
    entry point, but before this fix the source never was, so a concurrent
    insert could resize it and free the array mid-walk. Under free-threaded
    CPython that was a reliable SIGSEGV; the fix locks both objects with
    ``Py_BEGIN_CRITICAL_SECTION2``, which makes each walk atomic against the
    mutating thread and leaves no window to observe a torn table.
    """
    if getattr(any_multidict_class, "__module__", "").endswith("_multidict_py"):
        pytest.skip("Test is only applicable to the C extension")

    extenders, mutators = 8, 3
    source: MutableMultiMapping[str] = any_multidict_class(
        [(f"init-{i}", f"v{i}") for i in range(64)]
    )
    sizes: list[int] = []
    stop = threading.Event()
    # Start every thread together; without this the extenders can finish
    # before a mutator has resized anything and the race never opens.
    ready = threading.Barrier(extenders + mutators)

    def extender() -> None:
        ready.wait()
        for _ in range(200):
            # Every entry point that reads a second multidict: the copy
            # constructor, extend(), update() and merge().
            dst = any_multidict_class()
            dst.extend(source)
            dst.update(source)
            dst.merge(source)
            sizes.append(len(any_multidict_class(source)))

    def mutator(tag: str) -> None:
        # A private key namespace per thread, so every delete succeeds and the
        # loop needs no exception handling. Growing well past the load factor
        # and shrinking back forces repeated _md_resize() calls on the source,
        # which is what frees the array being walked.
        ready.wait()
        while not stop.is_set():
            for i in range(256):
                source[f"{tag}-{i}"] = str(i)
            for i in range(256):
                del source[f"{tag}-{i}"]

    extender_threads = [threading.Thread(target=extender) for _ in range(extenders)]
    mutator_threads = [
        threading.Thread(target=mutator, args=(f"grow{n}",)) for n in range(mutators)
    ]

    for t in extender_threads + mutator_threads:
        t.start()
    # Only stop resizing once every extender is done, so the source keeps
    # being reshaped underneath all of them and not just the slowest few.
    for t in extender_threads:
        t.join()
    stop.set()
    for t in mutator_threads:
        t.join()

    # Surviving without SIGSEGV is the point of the test. The sizes are a
    # cheap consistency check: the 64 seeded keys are never removed, so every
    # snapshot must have seen at least those.
    assert sizes
    assert min(sizes) >= 64
