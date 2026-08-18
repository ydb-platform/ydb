/* syntax version 1 */
-- Host→Guest 1-copy demo for heavy blob columns (PreferWasm).
--
-- Prerequisites:
--   1) upload: library sdk → UDF parse_blob
--   2) run setup.sql (CREATE TABLE) as a separate query
--
-- Auto-resident columns (v1) mark string columns that flow into WASM UDF
-- args from a table scan (KqpWideRead*). Literals / AS_TABLE do NOT get
-- PreferWasm — they fall back to host + CopyIntoCompartment.
--
-- Debug (stderr):
--   export YDB_WASM_STRING_DEBUG=1
-- Expected on the PreferWasm SELECT below:
--   [WasmString] GetCellValue: path=MakePreferWasm size=...
--   [WasmString] MakePreferWasm: destination=wasm via query_compartment ...
--   [WasmString] Make: destination=wasm_linear_memory ...
--   [WasmString] FillAbiStringArg: destination=reuse_wasm_resident ...
--   [WasmString] InvalidateGeneration: FreeBytes ...
--   [WasmString] InvalidateGeneration: generation=... freed=...
--
-- Run each block below as a SEPARATE query.

-- ========== seed ==========
UPSERT INTO parse_blob_demo (id, blob, unused_blob) VALUES
    (1u, "hello world, this payload is longer than fourteen bytes",
         "unused payload stays on host when not passed to WASM UDF"),
    (2u, "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
         "yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy"),
    (3u, NULL,
         "also unused");

-- ========== PreferWasm path ==========
-- Only `blob` is a WASM UDF arg → PreferWasm.
-- Expected (YDB_WASM_STRING_DEBUG=1):
--   MakePreferWasm / Make: destination=wasm_linear_memory
--   FillAbiStringArg: destination=reuse_wasm_resident
--   InvalidateGeneration: FreeBytes offset=...   ← free at query compartment teardown
SELECT
    id,
    ParseBlob::parse_blob(blob) AS parsed
FROM parse_blob_demo
ORDER BY id;

-- ========== seed_many (PreferWasm UnRef stress) ==========
-- 20 rows × 64-byte blobs. Run as its own query before the COUNT SELECT below.
UPSERT INTO parse_blob_demo (id, blob, unused_blob) VALUES
    (100u, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "u"),
    (101u, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "u"),
    (102u, "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc", "u"),
    (103u, "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd", "u"),
    (104u, "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", "u"),
    (105u, "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", "u"),
    (106u, "gggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggg", "u"),
    (107u, "hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh", "u"),
    (108u, "iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii", "u"),
    (109u, "jjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjj", "u"),
    (110u, "kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk", "u"),
    (111u, "llllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllll", "u"),
    (112u, "mmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmm", "u"),
    (113u, "nnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnn", "u"),
    (114u, "oooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo", "u"),
    (115u, "pppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppp", "u"),
    (116u, "qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq", "u"),
    (117u, "rrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrr", "u"),
    (118u, "ssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssss", "u"),
    (119u, "tttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttt", "u");

-- ========== PreferWasm mid-query free (no ORDER BY buffer) ==========
-- Stream through UDF without sorting so finished rows can UnRef before teardown.
-- Expected (YDB_WASM_STRING_DEBUG=1), interleaved:
--   Make: destination=wasm_linear_memory ...
--   FillAbiStringArg: reuse_wasm_resident ...
--   TryFree: destination=FreeBytes ...          ← UnRef of a finished row
-- and at the end either freed=0 or only leftovers:
--   InvalidateGeneration: FreeBytes ... / freed=N
--
-- Definitive UnRef-before-teardown check (no KQP buffering):
--   ./ya make -tA ydb/library/wasm/unittests -F '*UnRefFreesBeforeInvalidate*'
SELECT COUNT(*) FROM (
    SELECT ParseBlob::parse_blob(blob) AS parsed
    FROM parse_blob_demo
    WHERE id >= 100u AND blob IS NOT NULL
);

-- ========== host path for unused column ==========
SELECT id, unused_blob FROM parse_blob_demo ORDER BY id;

-- ========== literal (no PreferWasm; CopyIntoCompartment) ==========
SELECT ParseBlob::parse_blob(
    "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
) AS from_literal;

-- ========== AS_TABLE (no PreferWasm; CopyIntoCompartment) ==========
SELECT
    ParseBlob::parse_blob(blob) AS parsed
FROM AS_TABLE([
    <|blob: "hello world, this payload is longer than fourteen bytes"|>,
    <|blob: "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"|>,
    <|blob: NULL|>
]);
