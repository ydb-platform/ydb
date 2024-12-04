pragma config.flags("UdfIgnoreCase");
select 
    DATETIME::GETYEAR(Date('2001-01-01')),datetime::getmonth(Date('2001-01-01'))
