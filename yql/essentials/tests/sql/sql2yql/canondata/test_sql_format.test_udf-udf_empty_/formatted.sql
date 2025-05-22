/* postgres can not */
SELECT
    SimpleUdf::ReturnNull(''),
    SimpleUdf::ReturnVoid(''),
    SimpleUdf::ReturnEmpty(''),
    SimpleUdf::ReturnEmpty('') IS NULL
;
