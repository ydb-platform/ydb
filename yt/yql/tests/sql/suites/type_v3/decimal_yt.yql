pragma yt.UseNativeYtTypes = "true";
insert into plato.Output with truncate
select
    Decimal("3.1415", 5, 4), Decimal("2.9999999999", 12, 10), Decimal("2.12345678900876543", 35, 10),
    Decimal("nan", 5, 4), Decimal("nan", 15, 4), Decimal("nan", 35, 4),
    Decimal("inf", 5, 4), Decimal("inf", 15, 4), Decimal("inf", 35, 4),
    Decimal("-inf", 5, 4), Decimal("-inf", 15, 4), Decimal("-inf", 35, 4)
;