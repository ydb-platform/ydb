# Получение плана исполнения запроса и AST

Чтобы лучше понимать работу запросов, вы можете получить и проанализировать план запроса. Дополнительно к плану запроса вы можете получить AST (абстрактное синтаксическое дерево). Раздел AST содержит представление на внутреннем языке miniKQL.

## Получите плана запроса {#explain-plan}

{% include [intro.md](_includes/explain-plan/intro.md) %}

{% include [intro-exp.md](_includes/explain-plan/intro-exp.md) %}

## Получите AST {#ast}

Для получения AST добавьте в конце команды флаг `--ast`:

```bash
ydb -e grpcs://<эндпоинт> -d <база данных> \
table query explain -q "SELECT season_id, episode_id, title \
FROM episodes \
WHERE series_id = 1 AND season_id > 1 \
ORDER BY season_id, episode_id LIMIT 3" --ast
```

В результате дополнительно отобразится AST запроса.

{% include [examples.md](_includes/explain-plan/examples.md) %}

{% include [examples-exp.md](_includes/explain-plan/examples-exp.md) %}
