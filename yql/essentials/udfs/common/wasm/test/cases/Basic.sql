/* syntax version 1 */
$wasm_path = FilePath("add-mul-wasm");
$wasm = Wasm::Init($wasm_path);

SELECT
    $wasm.Run("add", "[10, 32]") AS add_result,
    $wasm.Run("mul", "[6, 7]") AS mul_result;
