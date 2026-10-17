SELECT
    ListFromRange(Decimal('10', 10, 0), Decimal('20', 10, 0), Decimal('2', 10, 0)) AS basic,
    ListFromRange(Decimal('2', 5, 2), Decimal('1', 5, 2), Decimal('-0.25', 5, 2)) AS reverse,
    ListFromRange(
        Decimal('0.00000000000000000000000000000000001', 35, 35),
        Decimal('0.00000000000000000000000000000000004', 35, 35),
        Decimal('0.00000000000000000000000000000000001', 35, 35)
    ) AS maximum_scale,
    ListFromRange(Decimal('0', 3, 0), Decimal('3', 3, 0)) AS default_step,
    ListFromRange(Decimal('0', 3, 0), Decimal('3', 3, 0), Decimal('0', 3, 0)) AS zero_step,
    ListFromRange(Decimal('nan', 3, 0), Decimal('3', 3, 0), Decimal('1', 3, 0)) AS nan_start,
    ListFromRange(Decimal('0', 3, 0), Decimal('inf', 3, 0), Decimal('1', 3, 0)) AS infinite_end,
    ListFromRange(Decimal('0', 3, 0), Decimal('3', 3, 0), Just(Decimal('1', 3, 0))) AS optional_step,
    ListFromRange(Decimal('0', 3, 0), Just(Decimal('3', 3, 0)), Decimal('1', 3, 0)) AS optional_end,
    ListFromRange(Just(Decimal('0', 3, 0)), Decimal('3', 3, 0), Decimal('1', 3, 0)) AS optional_begin,
    ListFromRange(Just(Decimal('0', 3, 0)), Just(Decimal('3', 3, 0)), Just(Decimal('1', 3, 0))) AS all_optional
;
