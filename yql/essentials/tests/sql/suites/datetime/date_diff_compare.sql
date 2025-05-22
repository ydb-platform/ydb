/* postgres can not */
select Date("2001-01-01") == Datetime("2001-01-01T00:00:00Z");
select Date("2001-01-01") == Timestamp("2001-01-01T00:00:00Z");
select Datetime("2001-01-01T00:00:00Z") == Timestamp("2001-01-01T00:00:00Z");

select Date("2001-01-01") != Datetime("2001-01-01T00:00:00Z");
select Date("2001-01-01") != Timestamp("2001-01-01T00:00:00Z");
select Datetime("2001-01-01T00:00:00Z") != Timestamp("2001-01-01T00:00:00Z");

select Date("2001-01-01") < Datetime("2001-01-01T00:00:00Z");
select Date("2001-01-01") < Timestamp("2001-01-01T00:00:00Z");
select Datetime("2001-01-01T00:00:00Z") < Timestamp("2001-01-01T00:00:00Z");

select Date("2001-01-01") <= Datetime("2001-01-01T00:00:00Z");
select Date("2001-01-01") <= Timestamp("2001-01-01T00:00:00Z");
select Datetime("2001-01-01T00:00:00Z") <= Timestamp("2001-01-01T00:00:00Z");

select Date("2001-01-01") > Datetime("2001-01-01T00:00:00Z");
select Date("2001-01-01") > Timestamp("2001-01-01T00:00:00Z");
select Datetime("2001-01-01T00:00:00Z") > Timestamp("2001-01-01T00:00:00Z");

select Date("2001-01-01") >= Datetime("2001-01-01T00:00:00Z");
select Date("2001-01-01") >= Timestamp("2001-01-01T00:00:00Z");
select Datetime("2001-01-01T00:00:00Z") >= Timestamp("2001-01-01T00:00:00Z");
