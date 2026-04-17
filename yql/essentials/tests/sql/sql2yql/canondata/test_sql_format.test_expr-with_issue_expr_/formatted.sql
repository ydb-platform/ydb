/* custom error: user message */
SELECT
    WithIssue(1 || 2, 'foo_file', 10, 20, 'user message')
;
