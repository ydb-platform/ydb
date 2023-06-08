# Реализация облачных разрешений через встроенные права {{ ydb-short-name }}

В {{ yandex-cloud }} управлением доступа к ресурсам занимается сервис IAM. Он помогает централизованно управлять правами доступа пользователей к ресурсам {{ yandex-cloud }}. IAM контролирует, чтобы все операции над ресурсами выполнялись только пользователями с необходимыми правами.
Чтобы указать, какие операции можно выполнять пользователю, ему необходимо назначить соответствующие роли. Например, разрешить просматривать ресурсы в облаке, управлять каталогом, и в частности, выполнять операции с ресурсами {{ ydb-short-name }}.

## Доступ к ресурсам {{ yandex-cloud }}

Чтобы предоставить пользователю доступ к ресурсу, ему назначаются роли на этот ресурс. Каждая роль состоит из набора разрешений, описывающих допустимые операции с ресурсом.

Перед тем, как выполнить операцию с ресурсом {{ ydb-short-name }}, в IAM будет отправлен запрос на проверку, разрешена ли эта операция. IAM сравнивает список необходимых разрешений со списком разрешений пользователя, выполняющего операцию. Если какого-то из разрешений у пользователя не окажется, операция не будет выполнена.

Роли в облаке бывают двух типов:
* *Примитивные роли* (`admin`, `editor`, `viewer`, `auditor`) - состоят из разрешений, применяемых ко всем типам ресурсов {{ yandex-cloud }}.
* *Сервисные роли* - состоят из разрешений, позволяющих выполнять операции с ресурсами определенного сервиса. Сервисную роль можно назначить на ресурс, для которого предназначена роль, или на ресурс, от которого наследуются права доступа.

Далее будут рассмотрены облачные роли для сервиса {{ ydb-short-name }} и то, каким образом они реализуются через внутренние права {{ ydb-short-name }}.

### Облачные роли для сервиса {{ ydb-short-name }}

* **ydb.auditor**
  Роль позволяет просматривать схемы таблиц, получать метаданные объектов схемы. Обладая такой ролью, пользователь не сможет получить доступ к содержимому таблиц. В частности, ему недоступна информация о границах разбиения ключей.
  Список разрешений, включенный в `ydb.auditor`:
  ```
  resource-manager.folders.get
  resource-manager.clouds.get
  ydb.databases.connect
  ydb.databases.{get,list}
  ydb.tables.list
  ydb.backups.get
  ydb.quotas.get
  ydb.databases.listAccessBindings
  ydb.backups.listAccessBindings
  ydb.schemas.getMetadata
  ```

* **ydb.viewer**
  Роль позволяет просматривать схемы таблиц, читать данные из таблиц баз данных {{ ydb-short-name }}.
  Список разрешений, включенный в `ydb.viewer`:
  ```
  Все разрешения ydb.auditor
  ydb.tables.select
  ```

* **ydb.editor**
  Роль позволяет просматривать схемы таблиц, создавать, изменять и удалять таблицы, читать, удалять и записывать данные в таблицы в базах {{ ydb-short-name }}.
  Список разрешений, включенный в `ydb.editor`:
  ```
  Все разрешения ydb.viewer
  ydb.backups.delete
  ydb.databases.create
  ydb.databases.alter
  ydb.databases.restore
  ydb.databases.backup
  ydb.databases.drop
  ydb.databases.start
  ydb.databases.stop
  ydb.tables.alter
  ydb.tables.create
  ydb.tables.delete
  ydb.tables.drop
  ydb.tables.update
  ydb.streams.write
  ```

* **ydb.admin**
  Роль позволяет выполнять все действия в {{ ydb-short-name }}.
  Список разрешений, включенный в `ydb.admin`:
  ```
  Все разрешения ydb.editor
  ydb.databases.{listAccessBindings,updateAccessBindings}
  ydb.backups.{listAccessBindings,updateAccessBindings}
  ```

### Встроенные права {{ ydb-short-name }}

В {{ ydb-short-name }} существует свой собственный набор прав, которым и управляется доступ к объектам схемы.
```
ydb.database.connect
ydb.tables.modify
ydb.tables.read
ydb.generic.list
ydb.generic.read
ydb.generic.write
ydb.generic.use_legacy
ydb.generic.use
ydb.generic.manage
ydb.generic.full_legacy
ydb.generic.full
ydb.database.create
ydb.database.drop
ydb.access.grant
ydb.granular.select_row
ydb.granular.update_row
ydb.granular.erase_row
ydb.granular.read_attributes
ydb.granular.write_attributes
ydb.granular.create_directory
ydb.granular.create_table
ydb.granular.create_queue
ydb.granular.remove_schema
ydb.granular.describe_schema
ydb.granular.alter_schema
```

Суть реализации облачных разрешений через встроенные права {{ ydb-short-name }} можно выразить следующими пунктами:
* Для каждого облачного разрешения создать группу пользователей внутри {{ ydb-short-name }}.
* Наделить такие группы встроенными правами, соответствующими облачным разрешениям.
* Добавлять в такие группы пользователей, обладающих необходимыми облачными разрешениями.

Для доступа к данным в {{ ydb-short-name }} проверяет следующие разрешения:
```
ydb.databases.connect
ydb.databases.list
ydb.schemas.getMetadata
ydb.databases.create
ydb.tables.select
```

Остальные разрешения ролей сервиса {{ ydb-short-name }} используются самим {{ yandex-cloud }} при управлении ресурсами баз данных через Control Plane.

## Реализация облачных ролей через встроенные права {{ ydb-short-name }}
Как было сказано выше, в реализации облачных ролей через встроенные права {{ ydb-short-name }} можно выделить 3 пункта:
* Для каждого облачного разрешения создать группу пользователей внутри {{ ydb-short-name }}.
* Наделить такие группы встроенными правами, соответствующими облачным разрешениям.
* Добавлять в такие группы пользователей, обладающих необходимыми облачными разрешениями.

Рассмотрим каждый пункт подробнее.

### Виртуальные группы

Первым шагом в реализации облачных ролей через права {{ ydb-short-name }} является создание так называемой виртуальной группы с именем вида `<cloud-permission-name>-<database-id>@as`.
* `cloud-permission-name` - Имя разрешения, включенного в одну из облачных ролей.
* `database-id` - Имя базы данных над которой выполняются операции.

В такие группы добавляются пользователи, роль которых включает разрешение `cloud-permission-name`.
Так как для доступа к данным проверяются следующие разрешения:
```
ydb.databases.connect
ydb.databases.list
ydb.schemas.getMetadata
ydb.databases.create
ydb.tables.select
```
для каждой базы данных в облаке, будут созданы виртуальные группы:
```
ydb.databases.connect-<database-id>@as
ydb.databases.list-<database-id>@as
ydb.schemas.getMetadata-<database-id>@as
ydb.databases.create-<database-id>@as
ydb.tables.select-<database-id>@as
```

### Наделение виртуальных групп правами

После того, как на предыдущем шаге были определены виртуальные группы, им назначаются встроенные права {{ ydb-short-name }}.
Каждое из проверяемых облачных разрешений жестко связано со встроенным правом {{ ydb-short-name }} следующим образом:
* `ydb.databases.connect` - `ydb.database.connect`
* `ydb.databases.list` - на данный момент никаких прав не назначается
* `ydb.schemas.getMetadata` - `ydb.generic.list`
* `ydb.databases.create` - `ydb.generic.use`
* `ydb.tables.select` - `ydb.generic.read`

Нужно заметить, что права для виртуальных групп назначаются на корень базы данных.
За создание виртуальных групп и назначение им прав, как за создание и управление ресурсами баз данных в облаке, отвечает специальный компонент Control Plane.

### Добавление пользователей в виртуальные группы

Для аутентификации пользователя в {{ yandex-cloud }} используется IAM токен. После предоставления пользователем IAM токена, компонент {{ ydb-short-name }}, отвечающий за проверку токенов, будет делать запросы в IAM сервис. Цель этих запросов - выяснить, обладает ли пользователь ролями, включающими в себя следующие разрешения:
```
ydb.databases.connect
ydb.databases.list
ydb.schemas.getMetadata
ydb.databases.create
ydb.tables.select
```
Нужно заметить, что на данный момент роль может быть назначена только на родительский ресурс (каталог или облако), роли которого наследуются вложенными ресурсами.
Если выяснится, что пользователь обладает ролью, включающую в себя одно из вышеперечисленных разрешений, то пользователь будет добавлен в соответствующую этому разрешению группу.

**Пример реализации облачных привилегий в {{ ydb-short-name }}**

Рассмотрим базу данных с id `123456789abcdef`.
Так как за управление ресурсами сервиса {{ ydb-short-name }} в облаке, отвечает специальный компонент Control Plane, то, в момент создания базы, этим компонентом будут созданы следующие виртуальные группы:
```
ydb.databases.connect-123456789abcdef@as
ydb.databases.list-123456789abcdef@as
ydb.schemas.getMetadata-123456789abcdef@as
ydb.databases.create-123456789abcdef@as
ydb.tables.select-123456789abcdef@as
```
Каждой из этих групп будут назначены соответствующие права на корень базы данных. При просмотре прав можно будет увидеть следующее:
```
ydb.databases.connect-123456789abcdef@as:ydb.database.connect
ydb.schemas.getMetadata-123456789abcdef@as:ydb.generic.list
ydb.databases.create-123456789abcdef@as:ydb.generic.use
ydb.tables.select-123456789abcdef@as:ydb.generic.read
```
Здесь отсутствует информация о группе `ydb.databases.list-123456789abcdef@as`, так как в данный момент этой группе не назначаются никакие права.

Предположим, пользователь обладает ролью `ydb.viewer` на каталог, в котором расположена база `123456789abcdef`. Тогда, при проверке разрешений `ydb.databases.connect`, `ydb.databases.list`, `ydb.schemas.getMetadata`, `ydb.databases.create`, `ydb.tables.select`, положительные ответы придут только для разрешений:
* `ydb.databases.connect`
* `ydb.databases.list`
* `ydb.schemas.getMetadata`
* `ydb.tables.select`

и пользователь будет добавлен в группы:
```
ydb.databases.connect-123456789abcdef@as
ydb.schemas.getMetadata-123456789abcdef@as
ydb.tables.select-123456789abcdef@as
ydb.databases.list-123456789abcdef@as
```
Права для этих групп описаны выше.
