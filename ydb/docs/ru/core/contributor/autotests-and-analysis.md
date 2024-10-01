# Автотесты и их стабильность

## История тестов {#test_history}

С историей выполнения тестов можно ознакомиться следующими путями

{% list tabs %}

- Через дашборд

  Откройте дашборд [Test history](https://datalens.yandex/4un3zdm0zcnyr?tab=A4)

  * Введите имя или путь теста в поле `full_name contain`, нажмите **Применить** - поиск выполняется по вхождению.


- Через отчет в PR

  Откройте [отчет в PR](suggest-change.md#test-results)

  * Для FAIL и MUTE тестов появляется столбец `history` содержащий статус последних 5 запусков этого теста в postcommit проверках
  * В контекстном меню теста можно выбрать и `Open test history`
  ![img](https://storage.yandexcloud.net/ydb-public-images/failed_tests_report.png)

{% endlist %}

### Как читать дашборд

![Пример](https://storage.yandexcloud.net/ydb-public-images/history_example.png)

* `Summary in 1 day window (Poscommit + Night Runs)` Показывает историю изменения статусов теста (Passed/Flaky/Muted stable/Muted flaky) в окне 1 дня

  * Можно определить как давно тест в таком состоянии

* `Test summary` - считает `succes rate` теста для всех типов workflow и build_type в которых участвовал

  * по нажатию на кнопку `Mute` можно замьютить тест

* `Test history` - показывает перечень всех запусков с ссылкой на GitHub action

  * можно найти в каких PR тест выполнялся и падал

### Как собирается история тестов

Факт запуска автотестов и статусы выполнения собираются в GitHub Action скриптом [upload_tests_results.py](../../../../../.github/scripts/analytics/upload_tests_results.py)

  * Запускается для каждого запуска тестов во всех типах проверок (Nightly, PR-Check, Postcommit)

Аналитика (стабильность, срезы по дням, определение owner'a тестов etc) собирается в GitHub Action через workflow [Collect-analytics-run](../../../../../.github/workflows/collect_analytics.yml)

  * Выполняется каждый час

{% cut "Данные хранятся в YDB QA Database"  %}

В проекте cloud [ydb-tech-cloud-prod/ydb_qa](https://console.yandex.cloud/folders/b1g0a77t7u7b81f3pu4o) завели [базу](https://console.yandex.cloud/folders/b1g0a77t7u7b81f3pu4o/ydb/databases/etnvsjbk7kh1jc6bbfi8/overview) для хранения результатов запусков тестов, аналитики, скорости сборки etc

Параметры подключения

`DATABASE_ENDPOINT = "grpcs://lb.etnvsjbk7kh1jc6bbfi8.ydb.mdb.yandexcloud.net:2135"`
`DATABASE_PATH = "/ru-central1/b1ggceeul2pkher8vhb6/etnvsjbk7kh1jc6bbfi8"`
token in yav by name `ydb-sa-1_cloud_token`

Пример загрузки данных [https://github.com/ydb-platform/benchhelpers/blob/82495e35c0bb1ad0a8d9c9cf475922c98c1916a8/tpcc/ydb/insert\_tpcc\_results.py](https://github.com/ydb-platform/benchhelpers/blob/82495e35c0bb1ad0a8d9c9cf475922c98c1916a8/tpcc/ydb/insert_tpcc_results.py)

{% endcut %}

### История тестов в testmo (треуется авторизация)

Каждый раз, когда тесты запускаются {{ ydb-short-name }} CI, их результаты загружаются в [приложение Test History](https://nebius.testmo.net/projects/view/1). В комментарии к результатам тестирования есть ссылка "Test history", ведущая на страницу с соответствующим прогоном в этом приложении.

В "Test history" члены команды {{ ydb-short-name }} могут просматривать тестовые прогоны, выполнять поиск тестов, просматривать логи и сравнивать их между различными тестовыми прогонами. Если какой-либо тест завершается сбоем на некоторой прекоммитной проверке, в его истории можно увидеть, был ли этот сбой вызван данным изменением, или тест был сломан ранее.

## Выключение и включение тестов CI (mute and un-mute)

Признак автотеста `muted` используется для того, чтобы не блокировать CI падениями сломанных или нестабильных тестов
С этим признаком тест должен быть возвращен в CI после починки/стабилизации либо удален

### Как найти и отключить тест (mute) {#how-to-mute}

{% list tabs %}

- Через дашборд

  Откройте дашборд [Test history](https://datalens.yandex/4un3zdm0zcnyr?tab=A4)

  1. Введите имя или путь теста в поле `full_name contain`, нажмите **Применить** - поиск выполняется по вхождению
  1. Нажмите ссылку `Mute`, которая создаст черновик issue в GitHub.
  ![img](https://storage.yandexcloud.net/ydb-public-images/Test_history_dash.png)

- Через отчет в PR

  Откройте [отчет в PR](suggest-change.md#test-results)

  1. В контекстном меню теста выберите `Create mute issue` ![screen](https://storage.yandexcloud.net/ydb-public-images/report_mute.png)

{% endlist %}

1. Добавьте issue в проект [Mute and Un-mute](https://github.com/orgs/ydb-platform/projects/45/views/6?visibleFields=%5B%22Title%22%2C%22Assignees%22%2C%22Status%22%2C126637100%5D). Для доступа к проекту требуется быть членом одной из [команд](https://github.com/orgs/ydb-platform/teams) {{ ydb-short-name }}
1. Установите `Status` на `Muted`
1. Установите поле `owner` = `%team_name%` (нарпример, на скриншоте: `Owner: TEAM:@ydb-platform/qp` имя команды `qp`). ![image.png](https://storage.yandexcloud.net/ydb-public-images/create_issue.png)
1. Откройте [muted_ya.txt](../../../../../.github/config/muted_ya.txt) в новой вкладке и отредактируйте его.
1. Скопируйте строку под `Add line to muted_ya.txt` (например, как на скриншоте, `ydb/core/kqp/ut/query KqpStats.SysViewClientLost`) и добавьте ее в [muted_ya.txt](../../../../../.github/config/muted_ya.txt), в какую часть файла добавлять не имеет значения.
1. Отредактируйте имя ветки куда хотите внести изменение, например, замените `{username}-patch-1` на `mute/{username}`. Это требуется только для того, чтобы проверка на имя ветки разрешила влитие
1. Создайте PR - скопируйте имя PR из имени issue.
1. Скопируйте описание issue в PR, сохраните строку `Не для changelog (запись в changelog не требуется)`.
1. Проверить, что в комментарии появилось сообщение `Muted new %N% tests` ![Muted new %N% tests](https://storage.yandexcloud.net/ydb-public-images/muted_new.png)
1. По ссылке проверить, что список тестов соответствует ожидаемому
1. Получите "Approve" от члена команды owner'a теста в PR
1. Влить.
1. Свяжите Issue и PR (поле "Development" в issue и PR)
1. Сообщите команде владельца теста о новых отключениях - в личном сообщении или в общем чате (с упоминанием ответственного за команду)

  * Членов команды можно определить перейдя по ссылке Owner

  * Чат для оповещения - YDB dev in GitHub , канал General, пример оповещения

      > мьючу 2 теста ydb/core/kqp/ut/olap/
      > [PR](https://github.com/ydb-platform/ydb/pull/9214)
      >
      > Owner: [TEAM:@ydb-platform/qp](https://github.com/orgs/ydb-platform/teams/qp)
      >
      > Summary history: in window 2024-09-13
      > Success rate 94.54545454545455%
      > Pass:52 Fail:3 Mute:0 Skip:0
      >
      > [history](https://datalens.yandex/34xnbsom67hcq?full_name=__in_ydb/core/kqp/ut/olap/KqpOlapAggregations.Aggregation_ResultCountAll_FilterL&full_name=__in_ydb/core/kqp/ut/olap/KqpOlapAggregations.Aggregation_ResultCountT_FilterL)

### Как включить тест (un-mute) {#how-to-unmute}

1. Откройте [muted_ya.txt](../../../../../.github/config/muted_ya.txt)
1. Нажмите "Edit" и удалите строку теста
1. Сохраните изменения (Отредактируйте ветку для слияния, например, замените `{username}-patch-1` на `mute/{username}`)
1. Отредактируйте имя PR как `Unmute {имя теста}`
1. Проверить, что в комментарии для сборки relwithdebinfo появилось сообщение `Unmuted %N% tests` ![сообщение](https://storage.yandexcloud.net/ydb-public-images/unmuted_tests.png)
1. По ссылке проверить, что список тестов соответствует ожидаемому
1. Получите "Approve" от члена команды владельца теста в PR
1. Merge
1. Если у теста есть issue в [Mute and Un-mute](https://github.com/orgs/ydb-platform/projects/45/views/6?visibleFields=%5B%22Title%22%2C%22Assignees%22%2C%22Status%22%2C126637100%5D) в статусе `Muted` - Переместите его в `Unmuted`
1. Свяжите Issue и PR (поле "Development" в issue и PR)
1. Переместите issue в статус `Unmuted`

## Нестабильные тесты (Flaky)

### Кто и когда следит за нестабильными тестами

Дежурный инженер по CI проверяет нестабильные тесты один раз в каждый рабочий день.

1. Откройте дашборд [Flaky](https://datalens.yandex/4un3zdm0zcnyr).
1. Выполните разделы **[Отключить Нестабильный Тест](#mute-flaky)** и **[Тест больше не Flaky - включаем (un-mute)](#unmute-flaky)** один раз в день или по требованию.

### Отключение (mute) нестабильных тестов {#mute-flaky}

Откройте дашборд [Flaky](https://datalens.yandex/4un3zdm0zcnyr)

1. Выберите сегодняшнюю дату.
2. Посмотрите на тесты в таблице "Mute candidate".

![image.png](https://storage.yandexcloud.net/ydb-public-images/mute_candidate.png)

3. Выберите сегодняшнюю дату в `date_window`.
1. Выберите `days_ago_window = 1` (сколько дней назад от выбранного дня для расчета статистики).
    * Если вы хотите понять, как давно и как часто тест начал падать, вы можете кликнуть по ссылке `history` в таблице (загрузка может занять время) или выбрать `days_ago_window = 1`.
1. Нажмите ссылку `Mute`, которая создаст черновик issue в GitHub.
1. Выполните шаги из [Как отключить тест (mute)](#how-to-mute)


### Тест больше не Flaky - включаем (un-mute) {#unmute-flaky}

1. Откройте дашборд [Flaky](https://datalens.yandex/4un3zdm0zcnyr).
1. Посмотрите на тесты в таблице "Unmute candidate".

![image.png](https://storage.yandexcloud.net/ydb-public-images/unmute.png)

3. Если в столбце `summary:` показано `mute <= 3` и `success rate >= 98%` - **пора включить тест**.
1. Выполните шаги из [Как включить тест (un-mute)](#how-to-unmute)


## Управление отключенными тестами (muted) в команде {#how-to-manage}

### Как посмотреть стабильность тестов команды

 Если вы хотите получить больше информации о стабильности вашего теста, посетите [dashboard](https://datalens.yandex/4un3zdm0zcnyr?tab=8JQ) (заполните поле `owner`=`{your_team_name}`)
![image.png](https://storage.yandexcloud.net/ydb-public-images/test_analitycs_1.png)
![image.png](https://storage.yandexcloud.net/ydb-public-images/test_analitycs_2.png)

### Как найти issue по отключенным (muted) тестам

1. Откройте проект [Mute and Un-mute](https://GitHub.com/orgs/ydb-platform/projects/45/views/6?visibleFields=%5B%22Title%22%2C%22Assignees%22%2C%22Status%22%2C126637100%5D)
1. кликните по метке с именем вашей команды, например muted тесты [для команды qp](https://GitHub.com/orgs/ydb-platform/projects/45/views/6?filterQuery=owner%3Aqp)
1. Откройте issue `Mute {имя теста}`
1. Выполните [Как включить(un-mute) тест](#how-to-unmute)