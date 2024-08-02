# Recursive directory traversal on a cluster

Iterator over the tree of the target cluster, with the ability to accumulate state, usually a list of paths to tables.
For each node's descendants, user—defined functions are called, where the state can be accumulated; attributes and nodes for further traversal can be selected.

Specified as the `WalkFolders` function in the [FROM](../../select/from.md) clause.

Returns a single column `State` with the same type as `InitialState`.

**Mandatory parameters**:

1. Path — Path to the initial directory;

**Optional parameters**:

1. InitialState (Persistable). The state type must be serializable (for example, Callable or Resource cannot be used). By default, ListCreate(String) is used.

**Optional named parameters**:

1. RootAttributes (`String`) — a string containing a list of desired meta-attributes separated by semicolons (e.g., `"schema;row_count"`). Default is `""`.
1. PreHandler — a lambda function that is called for the list of descendants of the current directory after the `List` operation. It takes a list of nodes, the current state, the current traversal depth, and returns the next state.
    Signature: `(List<Struct<'Path':String, 'Type':String, 'Attributes':Yson>>, TypeOf(InitialState), Int32) —> TypeOf(InitialState)`
    TypeOf(InitialState) — the inferred type of InitialState.
    Default implementation: `($nodes, $state, $level) —> ($state)`.
1. ResolveHandler — a lambda function that is called after `PreHandler`, which takes the list of descendant links of the current directory, the current state, the list of requested attributes for the parent directory, and the current traversal depth. It returns a `Tuple<List<Tuple<String,String>>, TypeOf(InitialState)>` — a tuple of the list of links to be visited with the requested meta-attributes and the next state. If a link is broken, `WalkFolders` will ignore it, so you don't need to check for this in the handler.
    Signature: `(List<Struct<'Path':String, 'Type':String, 'Attributes':Yson>>, TypeOf(InitialState), List<String>, Int32) -> Tuple<List<Tuple<String,String>>, TypeOf(InitialState)>`.
    Default implementation:
    ```yql
    -- Resolve each link, requesting the same attributes as the parent directory.
    ($nodes, $state, $rootAttrList, $level) -> {
        $linksToVisit = ListMap($nodes, ($node) -> (($node.Path, $rootAttrList)));
        return ($linksToVisit, $state);
        }
    ```
1. DiveHandler — a lambda function that is called after `ResolveHandler`. It takes the list of descendant directories of the current directory, the current state, the list of requested attributes for the parent directory, and the current traversal depth. It returns a `Tuple<List<Tuple<String,String>>, TypeOf(InitialState)>` — a tuple of the list of directories to visit (after processing the current directory) with the requested meta-attributes and the next state. The obtained paths are placed in the traversal queue.
    Signature: `(List<Struct<'Path':String, 'Type':String, 'Attributes':Yson>>, TypeOf(InitialState), List<String>, Int32) -> Tuple<List<Tuple<String,String>>, TypeOf(InitialState)>`.
    Default implementation: 
    ```yql
    -- Visit each subdirectory, requesting the same attributes as the parent directory.
        ($nodes, $state, $rootAttrList, $level) -> {
        $nodesToDive = ListMap($nodes, ($node) -> (($node.Path, $rootAttrList)));
        return ($nodesToDive, $state);
        }
    ```
1. PostHandler — a lambda function that is called after `DiveHandler`. It takes the list of descendants of the current directory after resolving links, the current state, and the current traversal depth.
    Signature: `(List<Struct<'Path':String, 'Type':String, 'Attributes':Yson>>, TypeOf(InitialState), Int32) -> TypeOf(InitialState)`.
    Default implementation: `($nodes, $state, $level) -> ($state)`

{% note warning %}    

* WalkFolders can create a significant load on the master. It should be used with caution when dealing with attributes that contain large values (such as schema), and one should avoid traversing subtrees that are large in size and/or depth. Directory listing requests within a single WalkFolders invocation can be executed in parallel; when requesting attributes with large values, you should reduce the number of simultaneous requests using the pragma `yt.BatchListFolderConcurrency`.
* Handlers are executed via [EvaluateExpr](../../../builtins/basic.md#evaluate_expr_atom), which has a limitation on the number of YQL AST nodes. Therefore, it is not possible to use containers of extremely large size in the State. This limitation can be circumvented by either making multiple WalkFolders calls and merging the results, or by serializing the new state into a string without intermediate deserialization (e.g., JSON/Yson lines).
* The order of node traversal in the tree is not Depth-First Search (DFS) due to parallel directory listing calls.
* InitialState is used for inferring the types of handlers, and it must be specified explicitly. For instance, use `ListCreate(String)` instead of `[]`.

{% endnote %}

Recommendations for usage:

* It's recommended to work with the Attributes column using [Yson UDF](../../../udf/list/yson.md).
* In a single request, the result of the listing for each directory is cached. The same subtree can be quickly traversed again in another WalkFolders call if it's convenient.

## Examples {#examples}

**Recursively collect the paths** of all tables starting from the `initial_folder`:
``` yql
$postHandler = ($nodes, $state, $level) -> {
    $tables = ListFilter($nodes, ($x)->($x.Type = "table"));
    return ListExtend($state, ListExtract($tables, "Path"));
};

SELECT State FROM WalkFolders(`initial_folder`, $postHandler AS PostHandler);
```

**Recursively find the most recently** created table in the `initial_folder`:
```yql
$extractTimestamp = ($node) -> {
    $creation_time_str = Yson::LookupString($node.Attributes, "creation_time");
    RETURN DateTime::MakeTimestamp(DateTime::ParseIso8601($creation_time_str));
};
$postHandler = ($nodes, $maxTimestamp, $_) -> {
    $tables = ListFilter($nodes, ($node) -> ($node.Type == "table"));
    RETURN ListFold(
        $tables, $maxTimestamp,
        ($table, $maxTimestamp) -> (max_of($extractTimestamp($table), $maxTimestamp))
    );
};
$initialTimestamp = CAST(0ul AS Timestamp);
SELECT
    *
FROM WalkFolders(`initial_folder`, $initialTimestamp, "creation_time" AS RootAttributes, $postHandler AS PostHandler);
```

**Recursively collect the paths** of all tables up to 2 levels deep from the `initial_folder`:
```yql
$diveHandler = ($nodes, $state, $attrList, $level) -> {
    $paths = ListExtract($nodes, "Path");
    $pathsWithReqAttrs = ListMap($paths, ($x) -> (($x, $attrList)));

    $nextToVisit = IF($level < 2, $pathsWithReqAttrs, []);
    return ($nextToVisit, $state);
};

$postHandler = ($nodes, $state, $level) -> {
    $tables = ListFilter($nodes, ($x)->($x.Type = "table"));
    return ListExtend($state, ListExtract($tables, "Path"));
};

SELECT State FROM WalkFolders(`initial_folder`,
    $diveHandler AS DiveHandler, $postHandler AS PostHandler);
```

**Collect paths from all nodes** in the `initial_folder` without descending into subdirectories:
```yql
$diveHandler = ($_, $state, $_, $_) -> {
    $nextToVisit = [];
    RETURN ($nextToVisit, $state);
};
$postHandler = ($nodes, $state, $_) -> {
    $tables = ListFilter($nodes, ($x) -> ($x.Type = "table"));
    RETURN ListExtend($state, ListExtract($tables, "Path"));
};
SELECT
    State
FROM WalkFolders(`initial_folder`, $diveHandler AS DiveHandler, $postHandler AS PostHandler);

```

**Recursively collect the paths** of all broken (destination path does not exist) links from the `initial_folder`:
```yql
$resolveHandler = ($list, $state, $attrList, $_) -> {
    $broken_links = ListFilter($list, ($link) -> (Yson::LookupBool($link.Attributes, "broken")));
    $broken_links_target_paths = ListNotNull(
        ListMap(
            $broken_links,
            ($link) -> (Yson::LookupString($link.Attributes, "target_path"))
        )
    );
    $nextState = ListExtend($state, $broken_links_target_paths);
    -- WalkFolders игнорирует сломанные ссылки при разрешении
    $paths = ListTake(ListExtract($list, "Path"), 1);
    $pathsWithReqAttrs = ListMap($paths, ($x) -> (($x, $attrList)));
    RETURN ($pathsWithReqAttrs, $nextState);
};

SELECT
    State
FROM WalkFolders(`initial_folder`, $resolveHandler AS ResolveHandler, "target_path" AS RootAttributes);
```
**Recursively collect YSON** for each node from the `initial_folder`, containing `Type`, `Path`, and a dictionary Attributes with the node's attributes (`creation_time`, user attribute `foo`):
```yql
-- В случае, если нужно накопить очень большое состояние в одном запросе, можно хранить его в виде строки, чтобы обойти ограничение на количество узлов во время Evaluate.
$saveNodesToYsonString = ($list, $stateStr, $_) -> {
    RETURN $stateStr || ListFold($list, "", ($node, $str) -> ($str || ToBytes(Yson::SerializeText(Yson::From($node))) || "\n"));
};
$serializedYsonNodes =
    SELECT
        State
    FROM WalkFolders("//home/yql", "", "creation_time;foo" AS RootAttributes, $saveNodesToYsonString AS PostHandler);

SELECT
    ListMap(String::SplitToList($serializedYsonNodes, "\n", true AS SkipEmpty), ($str) -> (Yson::Parse($str)));

```
**Pagination of WalkFolders results**. Skip the first 200 paths, then collect 100 from `initial_folder`:
```yql
$skip = 200ul;
$take = 100ul;

$diveHandler = ($nodes, $state, $reqAttrs, $_) -> {
    $paths = ListExtract($nodes, "Path");
    $pathsWithReqAttrs = ListMap($paths, ($x) -> (($x, $reqAttrs)));

    $_, $collectedPaths = $state;
    -- заканчиваем обход, если набрали необходимое число узлов
    $nextToVisit =  IF(
        ListLength($collectedPaths) > $take, 
        [],
        $pathsWithReqAttrs
    );
    return ($nextToVisit, $state);
};

$postHandler = ($nodes, $state, $_) -> {
    $visited, $collectedPaths = $state;
    $paths = ListExtract($nodes, "Path");
    $itemsToTake = IF(
        $visited < $skip, 
        0,
        $take - ListLength($collectedPaths)
    );
    $visited = $visited + ListLength($paths);

    return ($visited, ListExtend($collectedPaths, ListTake($paths, $itemsToTake)));
};
$initialState = (0ul, ListCreate(String));

$walkFoldersRes = SELECT * FROM WalkFolders(`initial_folder`, $initialState, $diveHandler AS DiveHandler, $postHandler AS PostHandler);

$_, $paths = Unwrap($walkFoldersRes);
select $paths;
```