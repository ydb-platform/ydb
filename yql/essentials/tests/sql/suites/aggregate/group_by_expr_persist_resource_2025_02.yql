/* custom error: Expected hashable and equatable type for distinct column */
select FormatType(TypeOf(some(distinct DateTime::ShiftMonths(x,1)))) from (select CurrentUtcDate() as x);

/* custom error: Expected hashable and equatable type for key column */
select FormatType(TypeOf(y)) from (select CurrentUtcDate() as x) group by DateTime::ShiftMonths(x,1) as y;
