/* syntax version 1 */
SELECT
    LocalUdf::udf_add(20, 22) AS add_result,
    LocalUdf::udf_strlen("banana") AS strlen_result;
