# Рекурсивный обход директорий на кластере

Итератор по дереву целевого кластера, с возможностью накопить состояние, обычно список путей к таблицам.
Для потомков каждого узла вызываются пользовательские функции, где можно накопить состояние; выбрать атрибуты и узлы для дальнейшего обхода.

Указывается как функция `WalkFolders` в [FROM](./from.md).

Возвращает одну колонку `State` с таким же типом, как и у `InitialState`.

Обязательные:

1. **Path** - Путь к начальной директории;

Опциональные:

2. **InitialState** (`Persistable`). Тип состояния должен быть любым сериализуемым (например, `Callable` или `Resource` нельзя использовать). По-умолчанию используется `ListCreate(String)`

Опциональные именованные:

3. **RootAttributes** (`String`) -  строка со списком интересующих мета-атрибутов через точку с запятой (Пример: `"schema;row_count"`). По-умолчанию `""`.
4. **PreHandler**  - лямбда, которая вызывается для списка потомков текущей директории после операции List (ссылки еще не зарезолвлены). Принимает список нод, текущее состояние, текущую глубину обхода, возвращает следующее состояние.

    Сигнатура: `(List<Struct<'Path':String, 'Type':String, 'Attributes':Yson>>, TypeOf(InitialState), Int32) -> TypeOf(InitialState)`
    TypeOf(InitialState) - выведенный тип InitialState.

    Реализация по-умолчанию: `($nodes, $state, $level) -> ($state)`

5. **ResolveHandler** - лямбда, которая вызывается после `PreHandler`, принимает список ссылок-потомков текущей директории, текущее состояние, список запрошенных атрибутов для директории-предка, текущую глубину обхода. Возвращает `Tuple<List<Tuple<String,String>>, TypeOf(InitialState)>` - тапл из списка ссылок, которые нужно посетить с запрашиваемыми мета-атрибутами и следующее состояние. Если ссылка сломана, WalkFolders ее проигнорирует, проверять на это в хендлере не нужно

    Сигнатура: `(List<Struct<'Path':String, 'Type':String, 'Attributes':Yson>>, TypeOf(InitialState), List<String>, Int32) -> Tuple<List<Tuple<String,String>>, TypeOf(InitialState)>`

    Реализация по-умолчанию:
    ```yql
    -- резловим каждую ссылку, запрашивая те же атрибуты, которые запросили у их предка
    ($nodes, $state, $rootAttrList, $level) -> {
            $linksToVisit = ListMap($nodes, ($node) -> (($node.Path, $rootAttrList)));
            return ($linksToVisit, $state);
        }```

6. **DiveHandler** - лямбда, которая вызывается после `ResolveHandler`, принимает список директорий-потомков текущей директории, текущее состояние, список запрошенных атрибутов для директории-предка, текущую глубину обхода. Возвращает `Tuple<List<Tuple<String,String>>, TypeOf(InitialState)>` - тапл из списка директорий, которые нужно посетить (после обработки текущей директории) с запрашиваемыми мета-атрибутами и следующее состояние. Полученные пути ставятся в очередь на обход.

    Сигнатура: `(List<Struct<'Path':String, 'Type':String, 'Attributes':Yson>>, TypeOf(InitialState), List<String>, Int32) -> Tuple<List<Tuple<String,String>>, TypeOf(InitialState)>`

    Реализация по-умолчанию:
    ```yql
    -- посещаем каждую поддиректорию, запрашивая те же атрибуты, которые запросили у их предка
        ($nodes, $state, $rootAttrList, $level) -> {
            $nodesToDive = ListMap($nodes, ($node) -> (($node.Path, $rootAttrList)));
            return ($nodesToDive, $state);
        }```
7. **PostHandler** - лямбда, которая вызывается после `DiveHandler`, принимает список потомков текущей директории после разрешения ссылок, текущее состояние, текущую глубину обхода.

    Сигнатура: `(List<Struct<'Path':String, 'Type':String, 'Attributes':Yson>>, TypeOf(InitialState), Int32) -> TypeOf(InitialState)`

    Реализация по-умолчанию: `($nodes, $state, $level) -> ($state)`

{% note warning %}

* **WalkFolders может создавать большую нагрузку на мастер.** Следует с осторожностью использовать WalkFolders с атрибутами, содержащими большие значения, (`schema` может быть одним из таких); обходить поддерево большого размера и/или глубины.

    Запросы листинга директорий внутри одного вызова WalkFolders могут выполняться параллельно, при запросе атрибутов с большими значениями нужно **уменьшить** количество одновременных запросов прагмой [`yt.BatchListFolderConcurrency`](../pragma_yt.md#ytbatchlistfolderconcurrency).

* Хендлеры выполняются через [EvaluateExpr](../../../builtins/basic.md#evaluate_expr_atom), существует ограничение на количество узлов YQL AST. Использовать в State контейнеры очень большого размера не получиться.

    Ограничение можно обойти несколькими вызовами WalkFolders с объединением результатов или сериализуя новое состояние в строку без промежуточной десериализации (например, JSON/Yson lines).

* Порядок обхода узлов в дереве не DFS из-за параллельных вызовов листинга директорий

* InitialState используется для вывода типов обработчиков, его необходимо указывать явно, например, `ListCreate(String)`, а не `[]`.

{% endnote %}

Рекомендации по использованию:

* C колонкой Attributes рекомендуется работать через [Yson UDF](../../../udf/list/yson.md)

* В одном запросе результат листинга для каждой директории кешируется, одно и то же поддерево можно быстро обойти заново в другом вызове WalkFolders, если так удобно

**Примеры:**

Собрать рекурсивно пути всех таблиц начиная из `initial_folder`:
``` yql
$postHandler = ($nodes, $state, $level) -> {
    $tables = ListFilter($nodes, ($x)->($x.Type = "table"));
    return ListExtend($state, ListExtract($tables, "Path"));
};

SELECT State FROM WalkFolders(`initial_folder`, $postHandler AS PostHandler);
```

Рекурсивно найти последнюю созданную таблицу в `initial_folder`:
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

Собрать рекурсивно пути всех таблиц в глубину на 2 уровня из `initial_folder`
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

Собрать пути из всех узлов в `initial_folder`, не заходя в поддиректории
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

Собрать рекурсивно пути всех сломанных (пути назначения не существует) ссылок из `initial_folder`.

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

Собрать рекурсивно Yson для каждого узла из `initial_folder`, содержащий `Type`, `Path`, словарь `Attributes` с атрибутами узла (`creation_time`, пользовательским атрибутом `foo`).
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

Пагинация результатов WalkFolders. Пропускаем 200 первых путей, собираем 100 из `initial_folder`:
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

