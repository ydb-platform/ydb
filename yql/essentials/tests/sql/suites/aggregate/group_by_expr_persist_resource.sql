select FormatType(TypeOf(some(distinct DateTime::ShiftMonths(x,1)))) from (select CurrentUtcDate() as x);
select FormatType(TypeOf(y)) from (select CurrentUtcDate() as x) group by DateTime::ShiftMonths(x,1) as y;
