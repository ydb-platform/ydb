/* syntax version 1 */
SELECT
    Domain,
    aggregate_list(AsStruct(DeviceID AS DeviceID, DeviceCategory AS DeviceCategory)) AS Devices
FROM (
    SELECT
        1 AS DeviceID,
        2 AS DeviceCategory,
        3 AS Domain
)
GROUP BY
    Domain
;
