/* syntax version 1 */
select
    Domain,
    aggregate_list(AsStruct(DeviceID as DeviceID, DeviceCategory as DeviceCategory)) as Devices
from (
    select 1 as DeviceID, 2 as DeviceCategory, 3 as Domain
)
group by Domain
