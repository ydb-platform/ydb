SELECT COUNT(*) FROM {table};
SELECT COUNT(*) FROM {table} WHERE AdvEngineID != 0;
SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM {table};
SELECT SUM(UserID) FROM {table};
SELECT COUNT(DISTINCT UserID) FROM {table};
SELECT COUNT(DISTINCT SearchPhrase) FROM {table};
SELECT MIN(EventDate), MAX(EventDate) FROM {table};
SELECT AdvEngineID, COUNT(*) as c FROM {table} WHERE AdvEngineID != 0 GROUP BY AdvEngineID ORDER BY c DESC;
SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM {table} GROUP BY RegionID ORDER BY u DESC LIMIT 10;
SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, avg(ResolutionWidth), COUNT(DISTINCT UserID) FROM {table} GROUP BY RegionID ORDER BY c DESC LIMIT 10;
-- q11
SELECT MobilePhoneModel, CountDistinctEstimate(UserID) AS u
FROM {table} WHERE MobilePhoneModel != '' GROUP BY MobilePhoneModel ORDER BY u DESC LIMIT 10;
-- q12
SELECT MobilePhone, MobilePhoneModel, CountDistinctEstimate(UserID) AS u
FROM {table} WHERE MobilePhoneModel != '' GROUP BY MobilePhone, MobilePhoneModel ORDER BY u DESC LIMIT 10;
-- q13
SELECT SearchPhrase, count(*) AS c
FROM {table} WHERE SearchPhrase != '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;
-- q14
SELECT SearchPhrase, CountDistinctEstimate(UserID) AS u
FROM {table} WHERE SearchPhrase != '' GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10;
-- q15
SELECT SearchEngineID, SearchPhrase, count(*) AS c
FROM {table} WHERE SearchPhrase != '' GROUP BY SearchEngineID, SearchPhrase ORDER BY c DESC LIMIT 10;
-- q16
SELECT UserID, count(*) AS c FROM {table} GROUP BY UserID ORDER BY c DESC LIMIT 10;
-- q17
SELECT UserID, SearchPhrase, count(*) AS c FROM {table} GROUP BY UserID, SearchPhrase ORDER BY c DESC LIMIT 10;
-- q18
SELECT UserID, SearchPhrase, count(*) AS c FROM {table} GROUP BY UserID, SearchPhrase LIMIT 10;
-- q19
SELECT UserID, m, SearchPhrase, count(*) AS c
FROM {table} GROUP BY UserID, DateTime::GetMinute(EventTime) AS m, SearchPhrase ORDER BY c DESC LIMIT 10;
-- q20
SELECT UserID FROM {table} WHERE UserID = 12345678901234567890;
-- q21
SELECT count(*) FROM {table} WHERE URL LIKE '%metrika%';
-- q22
SELECT SearchPhrase, some(URL), count(*) AS c
FROM {table} WHERE URL LIKE '%metrika%' AND SearchPhrase != '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;
-- q23
SELECT SearchPhrase, some(URL), some(Title), count(*) AS c, CountDistinctEstimate(UserID)
FROM {table} WHERE Title LIKE '%Яндекс%' AND URL NOT LIKE '%.yandex.%' AND SearchPhrase != ''
GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;
-- q24
SELECT * FROM {table} WHERE URL LIKE '%metrika%' ORDER BY EventTime LIMIT 10;
-- q25
SELECT SearchPhrase, EventTime FROM {table} WHERE SearchPhrase != '' ORDER BY EventTime LIMIT 10;
-- q26
SELECT SearchPhrase FROM {table} WHERE SearchPhrase != '' ORDER BY SearchPhrase LIMIT 10;
-- q27
SELECT SearchPhrase, EventTime FROM {table} WHERE SearchPhrase != '' ORDER BY EventTime, SearchPhrase LIMIT 10;
-- q28
SELECT CounterID, avg(length(URL)) AS l, count(*) AS c
FROM {table} WHERE URL != '' GROUP BY CounterID HAVING count(*) > 100000 ORDER BY l DESC LIMIT 25;
-- q29
SELECT key, avg(length(Referer)) AS l, count(*) AS c, some(Referer)
FROM {table} WHERE Referer != '' GROUP BY Url::CutWWW(Url::GetHost(Referer)) AS key
HAVING count(*) > 100000 ORDER BY l DESC LIMIT 25;
-- q30
SELECT
    sum(ResolutionWidth), sum(ResolutionWidth + 1), sum(ResolutionWidth + 2), sum(ResolutionWidth + 3),
    sum(ResolutionWidth + 4), sum(ResolutionWidth + 5), sum(ResolutionWidth + 6), sum(ResolutionWidth + 7),
    sum(ResolutionWidth + 8), sum(ResolutionWidth + 9), sum(ResolutionWidth + 10), sum(ResolutionWidth + 11),
    sum(ResolutionWidth + 12), sum(ResolutionWidth + 13), sum(ResolutionWidth + 14), sum(ResolutionWidth + 15),
    sum(ResolutionWidth + 16), sum(ResolutionWidth + 17), sum(ResolutionWidth + 18), sum(ResolutionWidth + 19),
    sum(ResolutionWidth + 20), sum(ResolutionWidth + 21), sum(ResolutionWidth + 22), sum(ResolutionWidth + 23),
    sum(ResolutionWidth + 24), sum(ResolutionWidth + 25), sum(ResolutionWidth + 26), sum(ResolutionWidth + 27),
    sum(ResolutionWidth + 28), sum(ResolutionWidth + 29), sum(ResolutionWidth + 30), sum(ResolutionWidth + 31),
    sum(ResolutionWidth + 32), sum(ResolutionWidth + 33), sum(ResolutionWidth + 34), sum(ResolutionWidth + 35),
    sum(ResolutionWidth + 36), sum(ResolutionWidth + 37), sum(ResolutionWidth + 38), sum(ResolutionWidth + 39),
    sum(ResolutionWidth + 40), sum(ResolutionWidth + 41), sum(ResolutionWidth + 42), sum(ResolutionWidth + 43),
    sum(ResolutionWidth + 44), sum(ResolutionWidth + 45), sum(ResolutionWidth + 46), sum(ResolutionWidth + 47),
    sum(ResolutionWidth + 48), sum(ResolutionWidth + 49), sum(ResolutionWidth + 50), sum(ResolutionWidth + 51),
    sum(ResolutionWidth + 52), sum(ResolutionWidth + 53), sum(ResolutionWidth + 54), sum(ResolutionWidth + 55),
    sum(ResolutionWidth + 56), sum(ResolutionWidth + 57), sum(ResolutionWidth + 58), sum(ResolutionWidth + 59),
    sum(ResolutionWidth + 60), sum(ResolutionWidth + 61), sum(ResolutionWidth + 62), sum(ResolutionWidth + 63),
    sum(ResolutionWidth + 64), sum(ResolutionWidth + 65), sum(ResolutionWidth + 66), sum(ResolutionWidth + 67),
    sum(ResolutionWidth + 68), sum(ResolutionWidth + 69), sum(ResolutionWidth + 70), sum(ResolutionWidth + 71),
    sum(ResolutionWidth + 72), sum(ResolutionWidth + 73), sum(ResolutionWidth + 74), sum(ResolutionWidth + 75),
    sum(ResolutionWidth + 76), sum(ResolutionWidth + 77), sum(ResolutionWidth + 78), sum(ResolutionWidth + 79),
    sum(ResolutionWidth + 80), sum(ResolutionWidth + 81), sum(ResolutionWidth + 82), sum(ResolutionWidth + 83),
    sum(ResolutionWidth + 84), sum(ResolutionWidth + 85), sum(ResolutionWidth + 86), sum(ResolutionWidth + 87),
    sum(ResolutionWidth + 88), sum(ResolutionWidth + 89)
FROM {table};
-- q31
SELECT SearchEngineID, ClientIP, count(*) AS c, sum(IsRefresh), avg(ResolutionWidth)
FROM {table} WHERE SearchPhrase != '' GROUP BY SearchEngineID, ClientIP ORDER BY c DESC LIMIT 10;
-- q32
SELECT WatchID, ClientIP, count(*) AS c, sum(IsRefresh), avg(ResolutionWidth)
FROM {table} WHERE SearchPhrase != '' GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10;
-- q33
SELECT WatchID, ClientIP, count(*) AS c, sum(IsRefresh), avg(ResolutionWidth)
FROM {table} GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10;
-- q34
SELECT URL, count(*) AS c FROM {table} GROUP BY URL ORDER BY c DESC LIMIT 10;
-- q35
SELECT UserID, URL, count(*) AS c FROM {table} GROUP BY UserID, URL ORDER BY c DESC LIMIT 10;
-- q36
SELECT x0, x1, x2, x3, count(*) AS c
FROM {table}
GROUP BY ClientIP as x0, ClientIP - 1 as x1, ClientIP - 2 as x2, ClientIP - 3 as x3 ORDER BY c DESC LIMIT 10;
-- q37
SELECT URL, count(*) AS PageViews
FROM {table}
WHERE
    CounterID = 62 AND EventDate >= Date('2013-07-01') AND EventDate <= Date('2013-07-31') AND DontCountHits == 0
    AND IsRefresh == 0 AND URL != ''
GROUP BY URL ORDER BY PageViews DESC LIMIT 10;
-- q38
SELECT Title, count(*) AS PageViews
FROM {table}
WHERE
    CounterID = 62 AND EventDate >= Date('2013-07-01') AND EventDate <= Date('2013-07-31') AND DontCountHits == 0 AND
    IsRefresh == 0 AND Title != ''
GROUP BY Title ORDER BY PageViews DESC LIMIT 10;
-- q39
SELECT URL, count(*) AS PageViews
FROM {table}
WHERE
    CounterID = 62 AND EventDate >= Date('2013-07-01') AND EventDate <= Date('2013-07-31') AND IsRefresh == 0 AND
    IsLink != 0 AND IsDownload == 0
GROUP BY URL ORDER BY PageViews DESC LIMIT 1000;
-- q40
SELECT TraficSourceID, SearchEngineID, AdvEngineID, Src, Dst, count(*) AS PageViews
FROM {table}
WHERE
    CounterID = 62 AND EventDate >=  Date('2013-07-01') AND EventDate <= Date('2013-07-31') AND IsRefresh == 0
GROUP BY
    TraficSourceID, SearchEngineID, AdvEngineID, IF(SearchEngineID = 0 AND AdvEngineID = 0, Referer, '') AS Src,
    URL AS Dst
ORDER BY PageViews DESC LIMIT 1000;
-- q41
SELECT URLHash, EventDate, count(*) AS PageViews
FROM {table}
WHERE
    CounterID = 62 AND EventDate >= Date('2013-07-01') AND EventDate <= Date('2013-07-31') AND IsRefresh == 0 AND
    TraficSourceID IN (-1, 6) AND RefererHash = Digest::Md5HalfMix('http://example.ru/')
GROUP BY URLHash, EventDate ORDER BY PageViews DESC LIMIT 100;
-- q42
SELECT WindowClientWidth, WindowClientHeight, count(*) AS PageViews
FROM {table}
WHERE
    CounterID = 62 AND EventDate >=  Date('2013-07-01') AND EventDate <=  Date('2013-07-31') AND IsRefresh == 0 AND
    DontCountHits == 0 AND URLHash = Digest::Md5HalfMix('http://example.ru/')
GROUP BY WindowClientWidth, WindowClientHeight ORDER BY PageViews DESC LIMIT 10000;
-- q43
SELECT Minute, count(*) AS PageViews
FROM {table}
WHERE
    CounterID = 62 AND EventDate >= Date('2013-07-01') AND EventDate <= Date('2013-07-02') AND IsRefresh == 0 AND
    DontCountHits == 0
GROUP BY DateTime::ToSeconds(EventTime)/60 As Minute ORDER BY Minute
