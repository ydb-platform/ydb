# Процесс разработки: работа над изменениями кода YDB

Этот раздел содержит пошаговый сценарий, который поможет вам выполнить необходимые шаги по настройке и узнать, как внести изменения в проект YDB. Этому сценарию не обязательно строго следовать, вы можете разработать свой собственный подход на основе предоставленной информации.

## Настройка окружения {#envsetup}

### Учетная запись на GitHub {#github_login}

Чтобы предлагать какие-либо изменения в исходном коде YDB, необходима учетная запись на GitHub. Зарегистрируйтесь на [github.com](https://github.com/), если вы еще этого не сделали.

### Пара ключей SSH {#ssh_key_pair}

* Для подключения к github вы можете использовать: ssh/token/ssh из yubikey/password и т.д. Рекомендуемый метод - ssh-ключи.
* Если у вас еще нет созданных ключей (или yubikey), то просто создайте новые ключи. Полные инструкции находятся на [этой странице GitHub](https://docs.github.com/en/authentication/connecting-to-github-with-ssh/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent#generating-a-new-ssh-key).
* Если у вас есть yubikey, вы можете использовать legacy ключ из yubikey:
  * Предположим, что у вас уже есть настроенный yubikey (или вы настроили yubikey локально)
  * На вашем ноутбуке: `skotty ssh-keys`
  * Загрузите ssh-ключ `legacy@yubikey` на github (через пользовательский интерфейс: https://github.com/settings/keys)
  * Проверьте подключение на ноутбуке: `ssh -T git@github.com`

#### Удаленная разработка

Если вы разрабатываете на удаленном компьютере, вы можете использовать ключ со своего ноутбука (сгенерированный или ключ от yubikey). Вам необходимо настроить переадресацию ключей. (Полные инструкции находятся на [этой странице GitHub](https://docs.github.com/en/authentication/connecting-to-github-with-ssh/using-ssh-agent-forwarding)).

Предположим, что ваш удаленный компьютер dev123456.search.yandex.net.

* на вашем ноутбуке добавьте переадресацию по ssh (`~/.ssh/config`):
```
Host dev123456.search.yandex.net
    ForwardAgent yes
```
* на удаленном компьютере добавьте в `~/.bashrc`:
```
if [[ -S "$SSH_AUTH_SOCK" && ! -h "$SSH_AUTH_SOCK" ]]; then
    ln -sf "$SSH_AUTH_SOCK" ~/.ssh/ssh_auth_sock;
fi
export SSH_AUTH_SOCK=~/.ssh/ssh_auth_sock;
```
* проверьте подключение: `ssh -T git@github.com`

### Git CLI {#git_cli}

У вас должна быть установлена утилита командной строки git для запуска команд из консоли. Посетите страницу [Downloads](https://git-scm.com/downloads) официального веб-сайта для получения инструкций по установке.

Чтобы установить ее в Linux/Ubuntu, запустите:

```
sudo apt-get update
sudo apt-get install git
```

### Зависимости сборки {#build_dependencies}

На компьютере разработчика должны быть установлены некоторые библиотеки.

Чтобы установить их под Linux/Ubuntu, запустите:

```
sudo apt-get update
sudo apt-get install libidn11-dev libaio-dev libc6-dev
```

### GitHub CLI (необязательно) {#gh_cli}

Использование GitHub CLI позволяет создавать Pull Request'ы и управлять репозиторием из командной строки. Вы также можете использовать GitHub UI для таких действий.

Установите GitHub CLI, как описано [на домашней странице](https://cli.github.com/). Для Linux Ubuntu вы можете перейти непосредственно к [https://github.com/cli/cli/blob/trunk/docs/install_linux.md#debian-ubuntu-linux-raspberry-pi-os-apt](https://github.com/cli/cli/blob/trunk/docs/install_linux.md#debian-ubuntu-linux-raspberry-pi-os-apt).

Выполните настройку аутентификации:

```
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

```
Tip: you can generate a Personal Access Token here https://github.com/settings/tokens
The minimum required scopes are 'repo', 'read:org', 'admin:public_key'.
? Paste your authentication token:
```

Откройте [https://github.com/settings/tokens](https://github.com/settings/tokens), нажмите  "Generate new token" / "Classic", поставьте галочки в четырех полях:
* Поле **`workflow`**
* Три других, как указано в подсказке:  "repo", "admin:public_key" and "read:org" (в разделе "admin:org")

И скопипастите показанный токен, чтобы завершить настройку GitHub CLI.

### Форк и клонирование репозитория {#fork_create}

Официальным репозиторием YDB является [https://github.com/ydb-platform/ydb](https://github.com/ydb-platform/ydb), расположенный под учетной записью организации YDB `ydb-platform`.

Создайте рабочий каталог:
```
mkdir -p ~/ydbwork
cd ~/ydbwork
```

Чтобы работать над изменениями кода YDB, вам нужно создать форк-репозиторий под своей учетной записью GitHub, и клонировать его локально.

{% list tabs %}

- GitHub CLI

  Существует команда GitHub CLI, которая выполняет все сразу:

  ```
  gh repo fork ydb-platform/ydb --default-branch-only --clone
  ```

  После завершения у вас будет форк репозитория YDB Git, клонированный в `~/ydbwork/ydb`.

- git

  На https://github.com/ydb-platform/ydb нажмите Fork (создайте свою собственную копию ydb-platform/ydb).
  ```
  git clone git@github.com:{your_name}/ydb.git
  ```

{% endlist %}

Форк репозитория - это мгновенное действие, однако клонирование на локальный компьютер требует некоторого времени для передачи около 650 МБ данных репозитория по сети.

### Настройка авторства коммитов {#author}

Запустите следующую команду из своего каталога репозитория, чтобы указать свое имя и адрес электронной почты для коммитов, отправляемых с помощью Git:

```
cd ~/ydbwork/ydb
```
```
git config --global user.name "Marco Polo"
git config --global user.email "marco@ydb.tech"
```

## Работа над изменением {#feature}

Чтобы начать работу над изменением, убедитесь, что шаги, указанные в разделе [Настройка окружения](#envsetup) выше, выполнены.

### Обновление транка {#fork_sync}

Обычно для начала работы над изменением требуется свежий транк. Синхронизируйте fork, чтобы получить его, выполнив следующую команду:

```
gh repo sync your_github_login/ydb -s ydb-platform/ydb
```

Эта команда выполняет удаленную синхронизацию на GitHub. Затем перенесите изменения в локальный репозиторий:

```
cd ~/ydbwork/ydb
```
```
git checkout main
git pull
```

### Создайте ветку разработки {#create_devbranch}

Создайте ветку разработки с помощью Git (замените "feature42" на название вашей ветки) и назначьте для нее upstream:

```
git checkout -b feature42
git push --set-upstream origin feature42
```

### Внесите изменения и коммиты {#commit}

Редактируйте файлы локально, используйте стандартные команды Git для добавления файлов, проверки статуса, выполнения коммитов и отправки изменений в ваш fork репозиторий:

```
git add .
```

```
git status
```

```
git commit -m "Implemented feature 42"
```

```
git push
```

### Создайте Pull Request в официальный репозиторий {#create_pr}

Когда изменения будут завершены и протестированы локально (см. [Ya Build and Test](build-ya.md)), запустите следующую команду из корневого каталога вашего репозитория, чтобы отправить Pull Request в официальный репозиторий YDB:

```
cd ~/ydbwork/ydb
```
```
gh pr create --title "Feature 42 implemented"
```

После ответа на некоторые вопросы Pull Request будет создан, и вы получите ссылку на его страницу на GitHub.com.

### Предварительные проверки {#precommit_checks}

Перед мержем изменений выполняются прекоммитные проверки Pull Reqeust'а. Вы можете увидеть их статус на странице Pull Request'а.

В рамках прекоммитных проверок YDB CI создает артефакты и запускает все тесты, публикуя результаты в качестве комментария к Pull Request'у.

Если вы не являетесь членом команды YDB, проверки не будут запущены до тех пор, пока член команды не рассмотрит ваши изменения и не одобрит PR для тестов, присвоив метку "Ok-to-test".

### Результаты тестирования {#test-results}

Вы можете кликнуть по количеству тестов в разных разделах комментария с результатами тестирования, чтобы перейти к простому HTML-отчету о тестировании. В этом отчете вы можете увидеть, какие тесты были пройдены неудачно/успешно, и получить доступ к их логам.

### История тестов {#test_history}

Каждый раз, когда тесты запускаются YDB CI, их результаты загружаются в [приложение Test History](https://nebius.testmo.net/projects/view/1). В комментарии к результатам тестирования есть ссылка "Test history", ведущая на страницу с соответствующим прогоном в этом приложении.

В "Test history" члены команды YDB могут просматривать тестовые прогоны, выполнять поиск тестов, просматривать логи и сравнивать их между различными тестовыми прогонами. Если какой-либо тест завершается сбоем на некоторой прекоммитной проверке, в его истории можно увидеть, был ли этот сбой вызван данным изменением, или тест был сломан ранее.

### Review и merge {#review}

Pull Request может быть замержен после получения Approve от члена команды YDB. Для коммуникации используются комментарии. После подтверждения  член команды YDB нажимает на кнопку "Merge".

### Обновление изменения {#update}

Если в вашем репозитории открыт Pull Reqeust для какой-либо ветки разработки, он будет обновляться каждый раз, когда вы выполняете push в эту ветку, с перезапуском проверок.

### Rebase changes {#rebase}

При возникновении конфликтов, вы можете сделать rebase своих изменений поверх текущего транка из официального репозитория. Чтобы сделать это, [обновите транк](#fork_sync) в вашем fork, перенесите состояние ветки `main` на локальный компьютер, и запустите команду rebase:

```
# Предполагается, что ваша активная ветка является вашей веткой разработки
gh repo sync your_github_login/ydb -s ydb-platform/ydb
git fetch origin main:main
git rebase main
```
