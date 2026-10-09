# Archived YDB scan provider

This directory preserves the previous YDB scan provider and its supporting
actors, expression nodes, protobuf messages and MiniKQL nodes. It is not included
in the active provider build, and current YDB code has no dependencies or runtime
registrations for it.

The active `ydb` provider lives in the parent directory and reads remote tables
through Query SDK and the native provider toolkit. The archived provider must
not be linked alongside it: its historical expression-node and protobuf names
are retained here only as source history.
