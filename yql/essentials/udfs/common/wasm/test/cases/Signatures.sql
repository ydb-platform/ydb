/* syntax version 1 */
$wasm_path = FilePath("scalar-wasm");
$wasm = Wasm::Init($wasm_path);

SELECT
    $wasm.Run("add", "[10, 32]") AS add_run,
    $wasm.Run("const42", "[]") AS const42,
    $wasm.Run("inc", "[5]") AS inc_result,
    $wasm.Run("square_f", "[3.0]") AS square_f,
    $wasm.Run("add_i32", "[7, 8]") AS add_i32,
    $wasm.Run("nop", "[]") AS nop_void,
    ListLength($wasm.Describe()) AS exports_count;
