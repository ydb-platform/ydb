/* syntax version 1 */
-- Expect: boom-from-wasm and WASM-only stack (boom_leaf → boom_middle → fail).
SELECT
    Throw::fail() AS x;
