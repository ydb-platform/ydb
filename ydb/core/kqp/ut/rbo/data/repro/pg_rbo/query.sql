-- This is the YQL-compatible form of the PostgreSQL-shaped repro.
-- The original ::timestamp/::numeric/::mvarchar/::bytea casts are represented
-- by native YQL casts and String values so the query reaches new RBO.
PRAGMA TablePathPrefix = "/Root/pg-rbo/";

SELECT DISTINCT
    T1._IDRRef,
    T1._Description,
    T1._Fld61627,
    T1._Fld61628,
    T1._Fld61629,
    T1._Fld61630,
    T1._Fld61632,
    T1._Fld61635RRef,
    T1._Fld61634,
    T1._Fld61636,
    T1._Fld61637RRef,
    T1._Fld61638RRef,
    CASE
        WHEN T1._Fld61634 = CAST(0 AS Timestamp) THEN CAST(0 AS Decimal(15, 0))
        WHEN T1._Fld61634 >= CAST(1000000 AS Timestamp) THEN CAST(0 AS Decimal(15, 0))
        ELSE CAST(1 AS Decimal(15, 0))
    END AS _age,
    COALESCE(CAST(T2._Q_001_F_006 AS String), ""),
    COALESCE(CAST(T3._Q_001_F_006 AS String), ""),
    COALESCE(CAST(T4._Q_001_F_006 AS String), ""),
    COALESCE(T5._Fld61623RRef, ""),
    COALESCE(T6._Q_001_F_001RRef, ""),
    COALESCE(T7._Q_001_F_001RRef, ""),
    COALESCE(T7._Q_001_F_002RRef, ""),
    COALESCE(CAST(T7._Q_001_F_005 AS String), ""),
    COALESCE(T7._Q_001_F_004RRef, ""),
    COALESCE(T7._Q_001_F_003, 0),
    COALESCE(T8._Q_001_F_001RRef, ""),
    COALESCE(T8._Q_001_F_002RRef, ""),
    COALESCE(CAST(T8._Q_001_F_005 AS String), ""),
    COALESCE(T8._Q_001_F_004RRef, ""),
    COALESCE(T8._Q_001_F_003, 0),
    COALESCE(T9._Q_001_F_001RRef, ""),
    COALESCE(T9._Q_001_F_002RRef, ""),
    COALESCE(CAST(T9._Q_001_F_003 AS String), ""),
    COALESCE(T9._Q_001_F_004RRef, ""),
    COALESCE(T9._Q_001_F_005, 0)
FROM `_Reference61509` AS T1
LEFT OUTER JOIN `tt19` AS T2 ON
    T1._IDRRef = T2._Q_001_F_000RRef
    AND T2._Q_001_F_002RRef = "ref-11"
LEFT OUTER JOIN `tt19` AS T3 ON
    T1._IDRRef = T3._Q_001_F_000RRef
    AND T3._Q_001_F_002RRef = "ref-13"
LEFT OUTER JOIN `tt19` AS T4 ON
    T1._IDRRef = T4._Q_001_F_000RRef
    AND T4._Q_001_F_002RRef = "ref-14"
LEFT OUTER JOIN `_InfoRg61621` AS T5 ON
    T5._Fld61622RRef = T1._IDRRef
    AND T5._Fld543 = CAST(0 AS Int32)
LEFT OUTER JOIN `tt22` AS T6 ON
    T6._Q_001_F_000RRef = T1._IDRRef
LEFT OUTER JOIN `tt41` AS T7 ON
    T7._Q_001_F_000RRef = T1._IDRRef
LEFT OUTER JOIN `tt35` AS T8 ON
    T8._Q_001_F_000RRef = T1._IDRRef
LEFT OUTER JOIN `tt29` AS T9 ON
    T9._Q_001_F_000RRef = T1._IDRRef
WHERE T1._Fld543 = CAST(0 AS Int32)
  AND EXISTS (
      SELECT 1
      FROM `tt16` AS T10
      WHERE T1._IDRRef = T10._Q_000_F_007RRef
  );
