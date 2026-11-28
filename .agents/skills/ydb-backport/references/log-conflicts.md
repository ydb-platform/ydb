# Conflict hunks

The script ../scripts/resolve_log_conflicts.py keeps the cherry-picked side of every hunk. That side is the text between `=======` and `>>>>>>>`. An empty side is kept too, so a line that exists only on the target branch is removed.

A nested marker is an error. The script exits with code 2 and does not rewrite that file.
