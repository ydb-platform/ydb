|Path format|Description|Example|
|----|----|---|
|Path ends with a `/`|Path to a directory|The path `/a` addresses all contents of the directory:<br/>`/a/b/c/d/1.txt`<br/>`/a/b/2.csv`|
|Path contains a wildcard character `*`|Any files nested in the path|The path `/a/*.csv` addresses files in directories:<br/>`/a/b/c/1.csv`<br/>`/a/2.csv`<br/>`/a/b/c/d/e/f/g/2.csv`|
|Path does not end with `/` and does not contain wildcard characters|Path to a single file|The path `/a/b.csv` addresses the specific file `/a/b.csv`|
