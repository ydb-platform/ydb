# Процесс разработки: работа над изменениями кода {{ ydb-short-name }}

Этот раздел содержит пошаговый сценарий, который поможет вам выполнить необходимые шаги по настройке и узнать, как внести изменения в проект {{ ydb-short-name }}. Этому сценарию не обязательно строго следовать, вы можете разработать свой собственный подход на основе предоставленной информации.

## Настройка окружения {#envsetup}

### Учетная запись на GitHub {#GitHub_login}

Чтобы предлагать какие-либо изменения в исходном коде {{ ydb-short-name }}, необходима учетная запись на GitHub. Зарегистрируйтесь на [GitHub](https://github.com/), если вы еще этого не сделали.

### Пара ключей SSH {#ssh_key_pair}

* Для подключения к GitHub вы можете использовать: ssh/token/ssh из yubikey/password и т.д. Рекомендуемый метод - ssh-ключи.
* Если у вас еще нет созданных ключей (или yubikey), то просто создайте новые ключи. Полные инструкции находятся на [этой странице GitHub](https://docs.github.com/en/authentication/connecting-to-github-with-ssh/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent#generating-a-new-ssh-key).
* Если у вас есть yubikey, вы можете использовать legacy ключ из yubikey:

  * Предположим, что у вас уже есть настроенный yubikey (или вы настроили yubikey локально)
  * На вашем ноутбуке: `skotty ssh keys`
  * Загрузите ssh-ключ `legacy@yubikey` на GitHub ([через пользовательский интерфейс](https://github.com/settings/keys))
  * Проверьте подключение на ноутбуке: `ssh -T git@github.com`

#### Удаленная разработка

Если вы разрабатываете на удаленном компьютере, вы можете использовать ключ со своего ноутбука (сгенерированный или ключ от yubikey). Вам необходимо настроить переадресацию ключей. (Полные инструкции находятся на [этой странице GitHub](https://docs.github.com/en/authentication/connecting-to-github-with-ssh/using-ssh-agent-forwarding)).

Предположим, что ваш удаленный компьютер dev123456.search.yandex.net.

* на вашем ноутбуке добавьте переадресацию по ssh (`~/.ssh/config`):

```text
Host dev123456.search.yandex.net
    ForwardAgent yes
```

* на удаленном компьютере добавьте в `~/.bashrc`:

```bash
if [[ -S "$SSH_AUTH_SOCK" && ! -h "$SSH_AUTH_SOCK" ]]; then
    ln -sf "$SSH_AUTH_SOCK" ~/.ssh/ssh_auth_sock;
fi
export SSH_AUTH_SOCK=~/.ssh/ssh_auth_sock;
```

* проверьте подключение: `ssh -T git@github.com`

### Git CLI {#git_cli}

У вас должна быть установлена утилита командной строки git для запуска команд из консоли. Посетите страницу [Downloads](https://git-scm.com/downloads) официального веб-сайта для получения инструкций по установке.

Чтобы установить ее в Linux/Ubuntu, запустите:

```bash
sudo apt-get update
sudo apt-get install git
```

### Зависимости сборки {#build_dependencies}

На компьютере разработчика должны быть установлены некоторые библиотеки.

Чтобы установить их под Linux/Ubuntu, запустите:

```bash
sudo apt-get update
sudo apt-get install libidn11-dev libaio-dev libc6-dev
```

### GitHub CLI (необязательно) {#gh_cli}

Использование GitHub CLI позволяет создавать Pull Request'ы и управлять репозиторием из командной строки. Вы также можете использовать GitHub UI для таких действий.

Установите GitHub CLI, как описано [на домашней странице](https://cli.github.com/). Для Linux Ubuntu вы можете перейти непосредственно к [https://github.com/cli/cli/blob/trunk/docs/install_linux.md#debian-ubuntu-linux-raspberry-pi-os-apt](https://github.com/cli/cli/blob/trunk/docs/install_linux.md#debian-ubuntu-linux-raspberry-pi-os-apt).

Выполните настройку аутентификации:

```bash
gh auth login
```

Вам будет задано несколько вопросов в интерактивном режиме, ответьте на них следующим образом:

|Вопрос|Ответ|
|--|--|
|What account do you want to log into?|**GitHub.com**|
|What is your preferred protocol for Git operations?|**SSH**|
|Upload your SSH public key to your GitHub account?|Выберите файл с открытым ключом (расширение `.pub`) из тех, что были созданы на шаге ["Создать пару ключей SSH"](#ssh_key_pair), например **/home/user/.ssh/id_ed25519.pub**|
|Title for your SSH key|**GitHub CLI** (оставьте значение по умолчанию)|
|How would you like to authenticate GitHub CLI|**Paste your authentication token**|

После последнего ответа вам будет предложено ввести токен, который вы можете сгенерировать в пользовательском интерфейсе GitHub:

```text
Tip: you can generate a Personal Access Token here https://github.com/settings/tokens
The minimum required scopes are 'repo', 'read:org', 'admin:public_key'.
? Paste your authentication token:
```

Откройте [настройки токенов в GitHub](https://github.com/settings/tokens), нажмите  "Generate new token" / "Classic", поставьте галочки в четырех полях:

* Поле **`workflow`**
* Три других, как указано в подсказке:  "repo", "admin:public_key" and "read:org" (в разделе "admin:org")

И скопипастите показанный токен, чтобы завершить настройку GitHub CLI.

### Форк и клонирование репозитория {#fork_create}

Официальным репозиторием {{ ydb-short-name }} является [https://github.com/ydb-platform/ydb](https://github.com/ydb-platform/ydb), расположенный под учетной записью организации YDB `ydb-platform`.

Чтобы работать над изменениями в {{ ydb-short-name }}, вы должны создать форк репозитория в вашем аккаунте GitHub. Нажмите на кнопку `Fork` на странице [официального репозитория {{ ydb-short-name }}](https://github.com/ydb-platform/ydb).

После того, как ваш форк создан, создайте локальный git репозиторий с двумя remote:

- `official`: официальный репозиторий {{ ydb-short-name }}, с ветками main и stable
- `fork`: ваш форк {{ ydb-short-name }} для разработки

```bash
mkdir -p ~/ydbwork
cd ~/ydbwork
git clone -o official git@github.com:ydb-platform/ydb.git
```

```bash
cd ydb
git remote add fork git@github.com:{your_github_user_name}/ydb.git
```

После завершения у вас будет форк репозитория {{ ydb-short-name }} Git, клонированный в `~/ydbwork/ydb`.

Форк репозитория - это мгновенное действие, однако клонирование на локальный компьютер требует некоторого времени для передачи около 650 МБ данных репозитория по сети.

Затем установите поведение по умолчанию для команды `git push`:

```bash
git config push.default current
git config push.autoSetupRemote true
```

Таким образом, команда `git push {remote}`будет автоматически устанавливать upstream `{remote}` для текущей ветки и последующие команды `git push` будут отправлять только текущую ветку.

Если вы собираетесь использовать GitHub CLI, установите `ydb-platform/ydb` репозиторием по умолчанию для GitHub CLI:

```bash
gh repo set-default ydb-platform/ydb
```

### Настройка авторства коммитов {#author}

Запустите следующую команду, чтобы указать свое имя и адрес электронной почты для коммитов, отправляемых с помощью Git (замените имя пользователя и email на ваши):

```bash
git config --global user.name "Marco Polo"
git config --global user.email "marco@ydb.tech"
```

## Работа над изменением {#feature}

Чтобы начать работу над изменением, убедитесь, что шаги, указанные в разделе [Настройка окружения](#envsetup) выше, выполнены.

### Обновление транка {#fork_sync}

Обычно для начала работы над изменением требуется последняя версия официального репозитория. Синхронизируйте вашу локальную ветку `main`, выполнив следующую команду:

Если ваша текущая локальная ветка `main`:

```bash
git pull --ff-only official main
```

Если ваша текущая локальная ветка не `main`:

```bash
cd ~/ydbwork/ydb
git fetch official main:main
```

Эта команда обновляет локальную ветку `main` без checkout.

### Создайте ветку разработки {#create_devbranch}

Создайте ветку разработки с помощью Git (замените "feature42" на название вашей новой ветки):

```bash
git checkout -b feature42
```

### Внесите изменения и коммиты {#commit}

Редактируйте файлы локально, используйте стандартные команды Git для добавления файлов, проверки статуса, выполнения коммитов и отправки изменений в ваш fork репозиторий:

```bash
git add .
git status
```

```bash
git commit -m "Implemented feature 42"
git push fork
```

Последующие push не требуют upstream или имени ветки:

```bash
git push
```

### Создайте Pull Request в официальный репозиторий {#create_pr}

Когда изменения будут завершены и протестированы локально (см. [Ya Build and Test](build-ya.md)), создайте Pull Request.

{% list tabs %}

- GitHub UI

  Откройте страницу вашей ветки на GitHub.com (`https://github.com/{your_github_user_name}/ydb/tree/{branch_name}`), нажмите `Contribute` и затем `Open Pull Request`.
  Также можно использовать ссылку в выводе команды `git push`, чтобы создать Pull Request:

  ```text
  ...
  remote: Resolving deltas: 100% (1/1), completed with 1 local object.
  remote:
  remote: Create a pull request for '{branch_name}' on GitHub by visiting:
  remote:      https://github.com/{your_github_user_name}/test/pull/new/{branch_name}
  ...
  ```

- GitHub CLI

  Установите и сконфигурируйте [GitHub CLI](https://cli.github.com/).

  ```bash
  cd ~/ydbwork/ydb
  ```

  ```bash
  gh pr create --title "Feature 42 implemented"
  ```

  После ответа на некоторые вопросы Pull Request будет создан, и вы получите ссылку на его страницу на GitHub.com.

{% endlist %}

### Предварительные проверки {#precommit_checks}

Перед мержем изменений выполняются прекоммитные проверки Pull Request'а.

Для изменений в коде {{ ydb-short-name }} прекоммитные проверки собирают артефакты, и запускают описанные в файлах `ya.make` тесты. Сборка/тесты выполняются на специальном merge коммите, который мержит ваши изменения с текущей веткой `main`.

Вы можете увидеть статус проверок на странице Pull Request'а. Также, ключевая информация о ходе сборки/тестов {{ ydb-short-name }} и текущий статус публикуются в комментариях к PR.

Если вы не являетесь членом команды {{ ydb-short-name }}, проверки не будут запущены до тех пор, пока член команды не рассмотрит ваши изменения, и не одобрит PR для тестов, присвоив метку `ok-to-test`.

Проверки перезапускаются каждый раз, когда пушатся новые изменения, предыдущая проверка прерывается, если она еще не завершена. Каждая итерация проверок создает собственный комментарий на странице PR, поэтому там сохраняется история проверок.

Участники команды разработки {{ ydb-short-name }} могут рестартовать проверки на новом merge коммите без пуша изменений. Для этого необходимо добавить к PR метку `rebase-and-check`.

### Результаты тестирования {#test-results}

Вы можете кликнуть по количеству тестов в разных разделах комментария с результатами тестирования, чтобы перейти к простому HTML-отчету о тестировании. В этом отчете вы можете увидеть, какие тесты были пройдены неудачно/успешно, и получить доступ к их логам.

#### Как отключить(mute) тест <a id="how-to-mute"></a>

- Через отчет PR
  - Откройте отчет в PR ![screen](https://storage.yandexcloud.net/ydb-public-images/report_mute.png)
  - В контекстном меню теста выберите `Create mute issue`

 - Через дашборд [Test history](https://datalens.yandex/4un3zdm0zcnyr?tab=A4)
  
    - Введите имя или путь теста в поле `full_name contain`, нажмите **Применить** - поиск выполняется по вхождению.  ![image.png](https://storage.yandexcloud.net/ydb-public-images/mute_candidate.png)

   - Нажмите ссылку `Mute`, которая создаст черновик issue в GitHub.


* Добавьте issue в проект [Mute and Un-mute](https://github.com/orgs/ydb-platform/projects/45/views/6?visibleFields=%5B%22Title%22%2C%22Assignees%22%2C%22Status%22%2C126637100%5D).
* Установите `статус` на `Отключен`
* Установите поле `владелец` на имя команды (см. issue для имени владельца). ![image.png](https://storage.yandexcloud.net/ydb-public-images/create_issue.png)
* Откройте [muted_ya.txt](https://github.com/ydb-platform/ydb/blob/main/.github/config/muted_ya.txt) в новой вкладке и отредактируйте его.
* Скопируйте строку под `Добавить строку в muted_ya.txt` (например, как на скриншоте, `ydb/core/kqp/ut/query KqpStats.SysViewClientLost`) и добавьте ее в [muted_ya.txt](https://github.com/ydb-platform/ydb/blob/main/.github/config/muted_ya.txt).
* Отредактируйте ветку для слияния, например, замените `{username}-patch-1` на `mute/{username}`.
* Создайте PR - скопируйте имя PR из имени issue.
* Скопируйте описание issue в PR, сохраните строку `Не для changelog (запись в changelog не требуется)`.
* Проверить, что в комментарии появилось сообщение ![Muted new %N% tests](https://storage.yandexcloud.net/ydb-public-images/muted_new.png)
* По ссылке проверить, что список тестов соответствует ожидаемому
* Получите "OK" от члена команды владельца теста в PR
* Влить.
* Свяжите Issue и Pr (поле "Development" в issue и PR)
* Сообщите команде владельца теста о новых отключениях - в личном сообщении или в общем чате (с упоминанием ответственного за команду)
* Ты молодец!

#### Как включить(un-mute) тест <a id="how-to-unmute"></a>
--Under construction--
* Откройте [muted_ya.txt](https://github.com/ydb-platform/ydb/blob/main/.github/config/muted_ya.txt)
* Нажмите "Edit" и удалите строку теста
* Сохраните изменения (Отредактируйте ветку для слияния, например, замените `{username}-patch-1` на `mute/{username}`)
* Отредактируйте имя PR как "Включить {имя теста}"
* Проверить, что в комментарии появилось ![сообщение](https://storage.yandexcloud.net/ydb-public-images/unmuted_tests.png)
* По ссылке проверить, что список тестов соответствует ожидаемому
* Получите "OK" от члена команды владельца теста в PR
* Merge
* Если у теста есть issue в [Mute and Un-mute](https://github.com/orgs/ydb-platform/projects/45/views/6?visibleFields=%5B%22Title%22%2C%22Assignees%22%2C%22Status%22%2C126637100%5D) в статусе `Muted` - Переместите его в `Unmuted`
* Свяжите Issue и Pr (поле "Разработка" в issue и PR)
* Переместите issue в статус `Unmuted`
* Ты молодец!

### Как управлять отключенными(muted) тестами в команде <a id="how-to-manage"></a>
--Under construction--
#### Изучите стабильность ваших тестов
 >Если вы хотите получить больше информации о стабильности вашего теста, посетите [dashboard](https://datalens.yandex/4un3zdm0zcnyr?tab=8JQ) (заполните поле `owner`=`{your_team_name}`)
![image.png](https://storage.yandexcloud.net/ydb-public-images/test_analitycs_1.png)
![image.png](https://storage.yandexcloud.net/ydb-public-images/test_analitycs_2.png)

#### Найдите ваши отключенные(muted) тесты
 >Не все muted тесты имеют issue в проекте github, мы работаем над этим
* Откройте проект [Mute and Un-mute](https://github.com/orgs/ydb-platform/projects/45/views/6?visibleFields=%5B%22Title%22%2C%22Assignees%22%2C%22Status%22%2C126637100%5D)
* кликните по метке с именем вашей команды, например [link to qp](https://github.com/orgs/ydb-platform/projects/45/views/6?filterQuery=owner%3Aqp) отключенные тесты (cgi `?filterQuery=owner%3Aqp`)
* Откройте issue `Отключить {имя теста}`
* Выполните [Как включить(un-mute) тест](#how-to-unmute)

### Нестабильные тесты (Flaky)

#### Кто и когда следит за нестабильными тестами

Дежурный инженер CI (в разработке) проверяет нестабильные тесты один раз в день (только в рабочие дни). 

- Откройте дашборд [Flaky](https://datalens.yandex/4un3zdm0zcnyr).
- Выполните разделы **[Отключить Нестабильный Тест](#mute-flaky)** и **[Тест Часто Нестабилен - Нужно Включить](#unmute-flaky)** один раз в день или по требованию

#### Отключить(mute) нестабильные тесты <a id="mute-flaky"></a>

Откройте дашборд [Flaky](https://datalens.yandex/4un3zdm0zcnyr).

- Выберите сегодняшнюю дату.
- Посмотрите на тесты в таблице "Кандидаты для Отключения".

![image.png](https://storage.yandexcloud.net/ydb-public-images/mute_candidate.png)

- Выберите сегодняшнюю дату в `date_window`.
- Выберите `days_ago_window = 1` (сколько дней назад от выбранного дня для расчета статистики).
  * Если вы хотите понять, как давно и как часто тест начал падать, вы можете кликнуть по ссылке `history` в таблице (загрузка может занять время) или выбрать `days_ago_window = 1`.
- Нажмите ссылку `Mute`, которая создаст черновик issue в GitHub.
- Выполните шаги из [Как отключить(mute) тест](#how-to-mute)
- Вы молодец!

### Тест больше не Flaky - включаем <a id="unmute-flaky"></a>

- Откройте дашборд [Flaky](https://datalens.yandex/4un3zdm0zcnyr).
- Посмотрите на тесты в таблице "Unmuted candidate".

![image.png](https://storage.yandexcloud.net/ydb-public-images/unmute.png)

- Если в столбце `summary:` показано `mute <= 3` и `success rate >= 98%` - **пора включить тест**.
- Выполните шаги из [Как включить(un-mute) тест ](#how-to-unmute)
- Вы молодец!

### История тестов {#test_history}

Каждый раз, когда тесты запускаются {{ ydb-short-name }} CI, их результаты загружаются в [приложение Test History](https://nebius.testmo.net/projects/view/1). В комментарии к результатам тестирования есть ссылка "Test history", ведущая на страницу с соответствующим прогоном в этом приложении.

В "Test history" члены команды {{ ydb-short-name }} могут просматривать тестовые прогоны, выполнять поиск тестов, просматривать логи и сравнивать их между различными тестовыми прогонами. Если какой-либо тест завершается сбоем на некоторой прекоммитной проверке, в его истории можно увидеть, был ли этот сбой вызван данным изменением, или тест был сломан ранее.
#### История тестов без авторизаций в testmo
- Через отчет PR
  - Откройте отчет в PR ![screen](https://storage.yandexcloud.net/ydb-public-images/report_mute.png)
  - В контекстном меню теста выберите `Open test history`

 - Через дашборд [Test history](https://datalens.yandex/4un3zdm0zcnyr?tab=A4)
  
    - Введите имя или путь теста в поле `full_name contain`, нажмите **Применить** - поиск выполняется по вхождению.  ![image.png](https://storage.yandexcloud.net/ydb-public-images/mute_candidate.png)
###### Как читать
![Пример](https://storage.yandexcloud.net/ydb-public-images/history_example.png)
* `Summary in 1 day window (Poscommit + Night Runs)` Показывает историю изменения статусов теста (Passed/Flaky/Muted stable/Muted flaky) в окне 1 дня
  * Можно определить как давно тест в таком состоянии
* `Test summary` - считает `succes rate` теста для всех типов workflow и build_type в которых участвовал
  * по нажатию на кнопку `Mute` можно замьютить тест
* `Test history` - показывает перечень всех запусков с ссылкой на github action
  * можно найти в каких PR тест падал



### Review и merge {#review}

Pull Request может быть замержен после получения Approve от члена команды {{ ydb-short-name }}. Для коммуникации используются комментарии. После подтверждения участник команды {{ ydb-short-name }} нажимает на кнопку "Merge".

### Обновление изменения {#update}

Если в вашем репозитории открыт Pull Request для какой-либо ветки разработки, он будет обновляться каждый раз, когда вы выполняете push в эту ветку, с перезапуском проверок.

### Rebase changes {#rebase}

При возникновении конфликтов, вы можете сделать rebase своих изменений поверх текущего транка из официального репозитория. Чтобы сделать это, [обновите транк](#fork_sync) на вашей локальной машине, и запустите команду rebase:

```bash
# Предполагается, что ваша активная ветка является вашей веткой разработки
git fetch official main:main
git rebase main
```

### Перенос патчей в стабильную ветку {#cherry_pick_stable}

Когда есть необходимость перенести патч в стабильную ветку, отведите ветку от ветки stable:

```bash
git fetch official
git checkout -b "cherry-pick-fix42" official/stable-24-1
```

Затем используйте cherry-pick для переноса патча и сделайте push ветки в ваш fork:

```bash
git cherry-pick {fixes_commit_hash}
git push fork
```

И создайте Pull Request из вашей ветки с патчем в стабильную ветку. Это делается аналогично открытию PR в ветку `main`, но необходимо убедиться что в качестве target выбрана нужная стабильная ветка.

Если вы используете GitHub CLI, target ветка указывается в опции `-B`:

```bash
gh pr create --title "Title" -B stable-24-1
```
