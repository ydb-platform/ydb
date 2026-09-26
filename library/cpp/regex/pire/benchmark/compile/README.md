# Benchmark компиляции Pire

Linux worker измеряет `Lexer::Parse().Compile<Pire::Scanner>()`. Матчинг и запуск
процесса в измеряемое время не входят; создание lexer, FSM, scanner и их уничтожение
входят. Вывод worker: status, суммарные ms, peak RSS KiB, сумма размеров scanner,
разделённые табуляцией. Бюджет при наличии переустанавливается для каждой компиляции.

```bash
ya make library/cpp/regex/pire/benchmark/compile -r
python3 library/cpp/regex/pire/benchmark/compile/run.py \
  --binary library/cpp/regex/pire/benchmark/compile/pire_compile_benchmark \
  --baseline /tmp/pire-before --repeat 5 > /tmp/pire-comparison.jsonl
```

Для baseline соберите тот же worker на исходной библиотеке, удалив только
`ScopedOperationBudget`, его optional-переменную и catch `BudgetExceeded` из
`main.cpp`. Сохраните бинарник вне build/worktree перед изменением библиотеки.
Общий цикл компиляции и allocator должны совпадать. Записывайте ревизию исходной
библиотеки и SHA-256 обоих бинарников вместе с результатами.

Runner сравнивает baseline (если задан), изменённую библиотеку без лимита,
с большим лимитом 10^15 и с ограничением `--budget` (по умолчанию 10000).
Порядок вариантов циклически меняется между повторами. Сравнивайте медианы
`compile_ms` только успешных запусков; для отказа используйте `elapsed_ms`.
Статусы signal/timeout/error нельзя считать успешной компиляцией или отказом
по бюджету.

Каждый sample запускается отдельным процессом: RLIMIT_AS 1 GiB, CPU 4 s
(жёсткий предел 5 s), wall 6 s, core dump отключён. Worker использует SYSTEM
allocator, совместимый с RLIMIT_AS. На тяжёлых regex используйте только runner.
ASan/MSan несовместимы с этим лимитом адресного пространства; для них используйте
unit-тесты, без runner. RSS включает весь процесс и allocator, это не размер DFA.

Корпус синтетический: повторы, вложенность, альтернативы, широкие классы,
экспоненциальная детерминизация с `{}` и без. Никакие production-данные не нужны.
CPU affinity и частота CPU не фиксируются; малые различия могут быть шумом.
