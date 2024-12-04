/* postgres can not */
select cast(cast(18000u as date) as string);
select cast(cast(18000u*86400ul+1u*3600u+2u*60u+3u as datetime) as string);
select cast(cast(18000u*86400000000ul+1u*3600000000ul+2u*60000000ul+3000000ul+4u as timestamp) as string);
select cast(cast(18000u*86400000000ul+1u*3600000000ul+2u*60000000ul+3000000ul+4u as interval) as string);

select cast(cast(49673u - 1u as date) as string);
select cast(cast(49673u as date) as string);

select cast(cast(49673u*86400ul - 1u as datetime) as string);
select cast(cast(49673u*86400ul as datetime) as string);

select cast(cast(49673u*86400000000ul - 1u as timestamp) as string);
select cast(cast(49673u*86400000000ul as timestamp) as string);

select cast(cast(49673u*86400000000ul - 1u as interval) as string);
select cast(cast(49673u*86400000000ul as interval) as string);