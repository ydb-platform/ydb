# Создание и изменение профиля

В настоящее время профили создаются и изменяются только в интерактивном режиме следующими командами:

``` bash
{{ ydb-cli }} init
```

или

``` bash
{{ ydb-cli }} config profile create [profile_name]
```

, где `[profile_name]` -- необязательное имя создаваемого или изменяемого профиля.


Первый шаг интерактивного сценария отличается в командах `init` и `profile create`:

{% list tabs %}

- Init

  Выводится перечень существующих профилей (если они есть), и предлагается выбор - создать новый (Create a new) или изменить конфигурацию одного из существующих:

   ```text
   Please choose profile to configure:
   [1] Create a new profile
   [2] test
   [3] local
   ```

   Если существующих профилей нет, или выбран вариант `1` на предыдущем шаге, то запрашивается имя профиля для создания:

   ``` text
   Please enter name for a new profile: 
   ```

   Если ввести в этот момент имя существующего профиля, то {{ ydb-short-name }} CLI переходит к шагам изменения его параметров, как если бы сразу была выбрана опция с именем этого профиля.

- Profile Create

   Если в командной строке не указано имя профиля, то оно запрашивается:
   ```text
   Please enter configuration profile name to create or re-configure:
   ```

{% endlist %}

Далее будет последовательно предложено выполнить действия с каждым параметром соединения, доступным для сохранения в профиле:

- Не сохранять в профиле - Don't save
- Задать или выбрать значение - Set a new value или Use <вариант>
- Оставить предыдущее значение - Use current value (опция доступна при изменении существующего профиля)

## Пример

Создание нового профиля `mydb1`:

1. Выполните команду:

    ```bash
    {{ ydb-cli }} config profile create mydb1
    ```

1. Введите [эндпоинт](../../../../concepts/connect.md#endpoint) или не сохраняйте этот параметр для профиля:

    ```text
    Pick desired action to configure endpoint:
     [1] Set a new endpoint value
     [2] Don't save endpoint for profile "mydb1"
    Please enter your numeric choice: 
    ```

1. Введите [имя базы данных](../../../../concepts/connect.md#database) или не сохраняйте этот параметр для профиля:

    ```text
    Pick desired action to configure database:
     [1] Set a new database value
     [2] Don't save database for profile "mydb1"
    Please enter your numeric choice: 
    ```

1. Выберите режим аутентификации или не сохраняйте этот параметр для профиля:

    ```text
    Pick desired action to configure authentication method:
     [1] Use IAM token (iam-token) cloud.yandex.com/docs/iam/concepts/authorization/iam-token
     [2] Use OAuth token of a Yandex Passport user (yc-token) cloud.yandex.com/docs/iam/concepts/authorization/oauth-token
     [3] Use metadata service on a virtual machine (use-metadata-credentials) cloud.yandex.com/docs/compute/operations/vm-connect/auth-inside-vm
     [4] Use security account key file (sa-key-file) cloud.yandex.com/docs/iam/operations/iam-token/create-for-sa
     [5] Don't save authentication data for profile "mydb1"
    Please enter your numeric choice:
    ```

    Если вы не уверены какой режим аутентификации выбрать, воспользуйтесь рецептом из статьи [Аутентификация](../../../../getting_started/auth.md) в разделе "Начало работы".

    Все доступные методы аутентификации описаны в статье [{#T}](../../../../concepts/auth.md). Набор методов и текст подсказок может отличаться от приведенного в данном примере.

    Если выбранный вами метод подразумевает указание дополнительного параметра, вам будет предложено его ввести. Например, если вы выбрали `4` (Use service account key file):

    ```text
    Please enter Path to service account key file (sa-key-file): 
    ```

1. На последнем шаге вам будет предложено активировать созданный профиль для использования по умолчанию. Ответьте 'n' (Нет), пока вы не прочитали статью про [Активацию профиля и применение активированного профиля](../activate.md):

    ```text
    Activate profile "mydb1" to use by default? (current active profile is not set) y/n: n
    ```
