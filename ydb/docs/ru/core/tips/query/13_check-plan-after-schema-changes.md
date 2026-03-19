# Проверка планов запросов после изменений схемы

## Проблема

После добавления индексов или изменения схемы данных планы запросов могут поменяться. Это происходит потому что оптимизатор начнёт учитывать новую схему данных:

Без проверки планов выполнения вы можете не заметить, что:
- Новые индексы не используются
- Используются старые, менее эффективные индексы
- Запросы выполняются полным сканированием вместо использования индекса
- Запросы выполняются с использованием нового индекса, но стали менее эффективными по другим причинам

## Решение

Добавьте в тесты планы выполнения наиболее важных запросов и отслеживайте их изменение в тестах.

**Рекомендации:**
- Проверяйте планы для всех критически важных запросов
- Сравнивайте планы до и после изменений схемы
- Документируйте неочевидные требования к плану запросов  

### Пример теста для отслеживания плана запроса

```go
func TestTableArrowBulkUpsert(t *testing.T) {
	db := dbConnection()
	ctx := context.Background()
	var plan string

	// Получаем план важного запроса
	err := db.Query().Exec(ctx, importantQuery,
		query.WithExecMode(query.ExecModeExplain),
		query.WithStatsMode(query.StatsModeNone, func(stats query.Stats) {
			plan = stats.QueryPlan()
		}),
		query.WithIdempotent(),
	)
	if err != nil {
		t.Fatal(err)
	}
	var jsPlan any
	if err := json.Unmarshal([]byte(plan), &jsPlan); err != nil {
		t.Fatal("failed unmarshaling json for query plan: ", err)
	}
	if planBytes, err := json.MarshalIndent(jsPlan, "", "  "); err == nil {
		plan = string(planBytes)
	} else {
		t.Fatal("failed marshaling json for query plan: ", err)
	}
	if strings.TrimSpace(plan) != expectedPlan {
		t.Fatalf("got\n%q, want\n%q", plan, expectedPlan)
	}
}
```
