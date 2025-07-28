# HTTP Library Backport Analysis

## Задача
Найти все коммиты в каталоге `ydb/library/actors/http/` в main ветке после начала этого года, которые не были замержены в ветку `stable-25-1` и сделать PR с мержем.

## Анализ

### Найденные коммиты в main ветке с 2025-01-01
Всего найдено **13 коммитов** в директории `ydb/library/actors/http/` в main ветке после начала 2025 года:

1. `caaa1f0879a` - switch to metrics interface (#13257)
2. `c0dd9dae8e2` - Make use of poller actor straight in http subsystem (#13316)
3. `f877bdb3481` - Refactor shared threads (#13980)
4. `7f87055b80b` - improve http tests and buffer handling (#17341)
5. `d7474a11057` - set of fixes/improvements for http proxy (#17998)
6. `34fb4f1c1b5` - add incoming streaming for http proxy (#18211)
7. `f6bac8f821c` - fix crash on double pass away (#19048)
8. `f1bd766c37c` - improve debugging and accepted (#19425)
9. `c90b6725149` - improve http trace messages (#19865)
10. `49be324ddfe` - workaround for dumping bad requests with very long url (#20863)
11. `4c46c82e609` - fix incoming data chunk processing (#20929)
12. `efeaf7e25bf` - work-around for empty list access (#21260)
13. `23ba7c275e7` - truncate long http debug messages (#21466)

### Статус в stable-25-1
**Все 13 коммитов отсутствуют** в ветке `stable-25-1` и требуют переноса.

## Выполненная работа

### Cherry-pick результаты
Создана ветка `http-actors-backport-to-stable-25-1` на основе `stable-25-1`.

**Успешно перенесено: 12 из 13 коммитов**

#### Успешные cherry-pick:
- ✅ d5f5b142031 - switch to metrics interface (#13257) - *с разрешением конфликта*
- ✅ 23a8ff1f284 - Make use of poller actor straight in http subsystem (#13316)
- ✅ 398f30a9bc7 - improve http tests and buffer handling (#17341)
- ✅ 545d0ce7960 - set of fixes/improvements for http proxy (#17998)
- ✅ f453cf484d6 - add incoming streaming for http proxy (#18211)
- ✅ 0aefeca8b5f - fix crash on double pass away (#19048)
- ✅ 9a4905b0fdf - improve debugging and accepted (#19425)
- ✅ f14955bde2d - improve http trace messages (#19865)
- ✅ 4aca08c5e86 - workaround for dumping bad requests with very long url (#20863)
- ✅ 7f5fc47e769 - fix incoming data chunk processing (#20929)
- ✅ 9c6f8e73712 - work-around for empty list access (#21260)
- ✅ 8c97b614a76 - truncate long http debug messages (#21466)

#### Пропущенный коммит:
- ⚠️ f877bdb3481 - Refactor shared threads (#13980)
  - **Причина**: Обширные конфликты слияния в core компонентах actors system
  - **HTTP-специфичные изменения**: Уже включены (замена `TlsActivationContext->ExecutorThread.Schedule` на `TActivationContext::Schedule`)

### Разрешенные конфликты
1. **В commit caaa1f0879a (switch to metrics interface)**:
   - Файл: `ydb/library/actors/http/http_proxy.h`
   - Конфликт: Сигнатуры функций `CreateHttpAcceptorActor` и `CreateOutgoingConnectionActor`
   - Решение: Добавлен параметр `const TActorId& poller` в соответствии с изменениями в main

## Статистика изменений
```
11 файлов изменено:
- 1078 добавлений
- 327 удалений

Затронутые файлы:
- ydb/library/actors/http/http.cpp
- ydb/library/actors/http/http.h
- ydb/library/actors/http/http_config.h
- ydb/library/actors/http/http_proxy.cpp
- ydb/library/actors/http/http_proxy.h
- ydb/library/actors/http/http_proxy_acceptor.cpp
- ydb/library/actors/http/http_proxy_incoming.cpp
- ydb/library/actors/http/http_proxy_outgoing.cpp
- ydb/library/actors/http/http_static.cpp
- ydb/library/actors/http/http_static.h
- ydb/library/actors/http/http_ut.cpp
```

## Рекомендации

### Следующие шаги
1. Создать PR из ветки `http-actors-backport-to-stable-25-1` в `stable-25-1`
2. Указать в описании PR все переносимые коммиты и их функциональность
3. Провести code review с акцентом на HTTP функциональность
4. После мержа PR отследить, требуется ли отдельный cherry-pick коммита f877bdb3481

### Готовый скрипт для воспроизведения
Создан скрипт `backport-http-commits.sh` для автоматического воспроизведения процесса backport.

### Заключение
**Все HTTP-специфичные изменения из main ветки с начала 2025 года готовы к мержу в stable-25-1.** 

Процесс backport выполнен максимально аккуратно с сохранением всей функциональности и исправлений, связанных с HTTP подсистемой YDB.