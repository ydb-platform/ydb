/* syntax version 1 */
$wasm_path = FilePath("memory-args-wasm");
$wasm = Wasm::Init($wasm_path);

SELECT
    $wasm.Run("sum_i64", "[1, 2, 3, 4]") AS list_sum,
    $wasm.Run("count_a", "[\"banana\"]") AS count_a,
    $wasm.Run("hello", "{\"args\": [], \"result\": \"string\"}") AS hello;
