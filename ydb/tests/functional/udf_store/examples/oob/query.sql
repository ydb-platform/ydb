/* syntax version 1 */
-- Intentional WASM memory traps. Expect query failure with a filtered WASM call stack.
--
-- Prerequisites:
--   upload UDF oob (required_libraries: [])
--
-- Path: InvokeUdfExport catches WAVM::Runtime::Exception* and rethrows with
-- exception type/args plus user-only frames (same filter as Throw::fail):
--   0. 0x… wasm!Oob!oob_leaf+… at …/main.cpp:N
--   1. 0x… wasm!Oob!oob_middle+… at …/main.cpp:N
--   2. 0x… wasm!Oob!crash+… at …/main.cpp:N
--
-- Note: in WASM, guest nullptr is offset 0 and is usually mapped, so a plain
-- `*nullptr` does not trap. null_deref uses null + large field offset instead.

-- ========== huge guest pointer (outOfBoundsMemoryAccess) ==========
SELECT Oob::crash() AS x;

-- ========== local buffer + huge index ==========
SELECT Oob::bad_index() AS x;

-- ========== null pointer + field (null_leaf → null_middle → null_deref) ==========
SELECT Oob::null_deref() AS x;

-- ========== poisoned / broken C++ reference (bad_ref_leaf → … → bad_ref) ==========
SELECT Oob::bad_ref() AS x;
