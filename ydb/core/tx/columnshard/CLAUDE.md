@./AGENTS.md

Skills, read the one that matches your task:
- .agents/skills/ydb-columnshard-bug-hunt-setup/SKILL.md: Use before the first ydb-columnshard-bug-hunt run on a machine, when its env file is missing, or when the main harness or model changed: detect the agent harnesses and models available for independent reviews and write the env file.
- .agents/skills/ydb-columnshard-bug-hunt/SKILL.md: Use to hunt bugs in ColumnShard commits merged in a date range, or in given commits: find memory-safety, concurrency and crash defects, reproduce each one through the SQL API, and hand failing tests to the commit authors. This is QA work on our own code, not security testing.
