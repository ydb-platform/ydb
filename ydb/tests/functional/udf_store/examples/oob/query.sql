/* syntax version 1 */
-- Intentional WASM memory traps. Expect query failure with WAVM call stack.
--
-- Prerequisites:
--   upload UDF oob (required_libraries: [])
--
-- Path: InvokeUdfExport catches WAVM::Runtime::Exception* and rethrows with
-- describeException(…), which ends with:
--   Call stack:
--     wasm!…!<leaf>+…
--     wasm!…!<middle>+…
--     wasm!…!<export>+…
--
-- Note: in WASM, guest nullptr is offset 0 and is usually mapped, so a plain
-- `*nullptr` does not trap. null_deref uses null + large field offset instead.
--
-- Compare with Throw::fail() (examples/throw): that path uses host ThrowException
-- and prints a filtered user-only stack (boom_leaf → boom_middle → fail).

-- ========== huge guest pointer (outOfBoundsMemoryAccess) ==========
SELECT Oob::crash() AS x;

-- ========== local buffer + huge index ==========
SELECT Oob::bad_index() AS x;

-- ========== null pointer + field (null_leaf → null_middle → null_deref) ==========
SELECT Oob::null_deref() AS x;

-- ========== poisoned / broken C++ reference (bad_ref_leaf → … → bad_ref) ==========
SELECT Oob::bad_ref() AS x;
