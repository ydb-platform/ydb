/* syntax version 1 */
$registry_path = FilePath("local_udf.function_descriptor.yson");
$registry = Wasm::LoadUdfs($registry_path);

SELECT
    $registry.Run("udf_add", "[20, 22]") AS add_result,
    $registry.Run("udf_strlen", "[\"banana\"]") AS strlen_result,
    ListLength($registry.Describe()) AS exports_count;
