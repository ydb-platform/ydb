/* hybridfile can not YQL-17763 */
select cast("-144170-12-31" as date32), cast(cast("-144170-12-31" as date32) as string);
select cast("-144169-01-01" as date32), cast(cast("-144169-01-01" as date32) as string);
select cast("-1-1-1" as date32), cast(cast("-1-1-1" as date32) as string);
select cast("0-1-1" as date32), cast(cast("0-1-1" as date32) as string);
select cast("1-1-1" as date32), cast(cast("1-1-1" as date32) as string);
select cast("1-02-29" as date32), cast(cast("1-02-29" as date32) as string);
select cast("1969-12-31" as date32), cast(cast("1969-12-31" as date32) as string);
select cast("1970-01-01" as date32), cast(cast("1970-01-01" as date32) as string);
select cast("2000-04-05" as date32), cast(cast("2000-04-05" as date32) as string);
select cast("2100-02-29" as date32), cast(cast("2100-02-29" as date32) as string);
select cast("2100-03-01" as date32), cast(cast("2100-03-01" as date32) as string);
select cast("2105-12-31" as date32), cast(cast("2105-12-31" as date32) as string);
select cast("2106-01-01" as date32), cast(cast("2106-01-01" as date32) as string);
select cast("148107-12-31" as date32), cast(cast("148107-12-31" as date32) as string);
select cast("148108-01-01" as date32), cast(cast("148108-01-01" as date32) as string);
