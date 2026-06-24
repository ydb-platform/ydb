/* postgres can not */
SELECT
    FormatType(EvaluateType(ParseTypeHandle('Int32' || '?')))
;
