select cast("-P106751616DT23H59M59.999999S" as interval64), cast(cast("-P106751616DT23H59M59.999999S" as interval64) as string);
select cast("P106751616DT23H59M59.999999S" as interval64), cast(cast("P106751616DT23H59M59.999999S" as interval64) as string);

select cast("-P106751616DT23H59M60S" as interval64), cast(cast("-P106751616DT23H59M60S" as interval64) as string);
select cast("P106751616DT23H59M60S" as interval64), cast(cast("P106751616DT23H59M60S" as interval64) as string);

select cast("P000000000DT00H00M00.000000S" as interval64), cast(cast("P000000000DT00H00M00.000000S" as interval64) as string);
select cast("PT0S" as interval64), cast(cast("PT0S" as interval64) as string);
select cast("-PT0S" as interval64), cast(cast("-PT0S" as interval64) as string);
select cast("PT0.000001S" as interval64), cast(cast("PT0.000001S" as interval64) as string);
select cast("-PT0.000001S" as interval64), cast(cast("-PT0.000001S" as interval64) as string);

select cast("PT0S" as interval64), cast(cast("PT0S" as interval64) as string);
select cast("PT0M" as interval64), cast(cast("PT0M" as interval64) as string);
select cast("PT0H" as interval64), cast(cast("PT0H" as interval64) as string);
select cast("P0D" as interval64), cast(cast("P0D" as interval64) as string);
select cast("P0W" as interval64), cast(cast("P0W" as interval64) as string);

select cast("PT999999S" as interval64), cast(cast("PT999999S" as interval64) as string);
select cast("PT999999M" as interval64), cast(cast("PT999999M" as interval64) as string);
select cast("PT999999H" as interval64), cast(cast("PT999999H" as interval64) as string);

select cast("P106751616D" as interval64), cast(cast("P106751616D" as interval64) as string);
select cast("P106751617D" as interval64), cast(cast("P106751617D" as interval64) as string);
select cast("P15250230W" as interval64), cast(cast("P15250230W" as interval64) as string);
select cast("P15250231W" as interval64), cast(cast("P15250231W" as interval64) as string);

select cast("PT0000000S" as interval64), cast(cast("PT0000000S" as interval64) as string);
select cast("PT0000000M" as interval64), cast(cast("PT0000000M" as interval64) as string);
select cast("PT0000000H" as interval64), cast(cast("PT0000000H" as interval64) as string);
select cast("P0000000000D" as interval64), cast(cast("P0000000000D" as interval64) as string);
select cast("P0000000000W" as interval64), cast(cast("P0000000000W" as interval64) as string);

select cast("PT1S" as interval64), cast(cast("PT1S" as interval64) as string);
select cast("PT1M" as interval64), cast(cast("PT1M" as interval64) as string);
select cast("PT1H" as interval64), cast(cast("PT1H" as interval64) as string);
select cast("P1D" as interval64), cast(cast("P1D" as interval64) as string);
select cast("P1W" as interval64), cast(cast("P1W" as interval64) as string);

select cast("-PT1S" as interval64), cast(cast("-PT1S" as interval64) as string);
select cast("-PT1M" as interval64), cast(cast("-PT1M" as interval64) as string);
select cast("-PT1H" as interval64), cast(cast("-PT1H" as interval64) as string);
select cast("-P1D" as interval64), cast(cast("-P1D" as interval64) as string);
select cast("-P1W" as interval64), cast(cast("-P1W" as interval64) as string);

