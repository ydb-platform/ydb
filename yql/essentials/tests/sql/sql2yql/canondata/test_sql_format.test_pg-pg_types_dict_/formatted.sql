SELECT
    ToSortedDict([([pgdate('2012-01-01')], 0), ([pgdate('2012-01-01')], 1)]),
    ToDict([([pgdate('2012-01-01')], 0), ([pgdate('2012-01-01')], 1)]),
    ToSortedMultiDict([([pgdate('2012-01-01')], 0), ([pgdate('2012-01-01')], 1)]),
    ToMultiDict([([pgdate('2012-01-01')], 0), ([pgdate('2012-01-01')], 1)])
;

SELECT
    ToSortedDict([([pgtext('2012-01-01')], 0), ([pgtext('2012-01-01')], 1)]),
    ToDict([([pgtext('2012-01-01')], 0), ([pgtext('2012-01-01')], 1)]),
    ToSortedMultiDict([([pgtext('2012-01-01')], 0), ([pgtext('2012-01-01')], 1)]),
    ToMultiDict([([pgtext('2012-01-01')], 0), ([pgtext('2012-01-01')], 1)])
;

SELECT
    ToSortedDict([([pgdate('2021-01-01')], 0), ([pgdate('1999-01-01')], 1)])
;

SELECT
    ToSortedDict([([pgtext('2021-01-01')], 0), ([pgtext('1999-01-01')], 1)])
;
