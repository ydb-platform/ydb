/* custom error: user message */
DEFINE SUBQUERY $sub() AS
    SELECT
        1 || 2
    ;
END DEFINE;

$sub = ($world) -> {
    RETURN WithIssue($sub($world), 'foo_file', 10, 20, 'user message');
};

PROCESS $sub();
