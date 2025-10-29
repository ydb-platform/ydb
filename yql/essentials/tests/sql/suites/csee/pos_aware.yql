/* custom error: <main>:5:13: Error: Cannot parse string value from entity (#) */
pragma DebugPositions;
select
      Yson::ConvertToString(d["answer"]),
      Yson::ConvertToString(d["query"])
from (
     select "{answer=foo;query=#}"y as d
)
