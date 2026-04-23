# Замена диска в кластере {{ ydb-short-name }}

В этом руководстве описан процесс замены диска в кластере {{ ydb-short-name }}, который развернут в соответствии с топологией `3-nodes-mirror-3-dc`, с помощью [Ansible](../../devops/deployment-options/ansible/index.md).

## Подготовка к замене диска

### Перед началом работы убедитесь, что выполнены условия

- кластер {{ ydb-short-name }} развернут по топологии `3-nodes-mirror-3-dc` (см. [топологии кластера](../../concepts/topology.md));

- на сервере кластера установлен жесткий диск на замену старому.

{% cut "Индикация состояния замененного диска красного цвета" %}

На скриншоте в интерфейсе мониторинга **Storage** → **Nodes** → **PDisks** проблемный физический диск отображается с красной индикацией — это признак неисправности или недоступности диска до замены.

![_](_assets/step-0-1.png)

{% endcut %}

### Определите `label` заменяемого диска

{% list tabs %}

- UI

  {% cut "Перейдите в UI-интерфейс {{ ydb-short-name }}" %}
  
  `https://ваш_ip_адрес:8765/monitoring/`

    {% cut "Нажмите **Storage** → **Nodes** → **PDisks**" %}

    ![_](_assets/step-1-1.png)

    {% endcut %}

  {% endcut %}
  
  ![_](_assets/step-1.png)

- Командная строка

  Команда и результат выполнения:

  ```bash
  root@static-node-1:/opt/ydb# ls /dev/disk/by-partlabel/
  ydb_disk_1 ydb_disk_2 ydb_disk_3 ydb_disk_4
  ```  
  
{% endlist %}

Этот `label` потребуется для выполнения следующего шага.

## Порядок действий

### Подготовьте диск к использованию

Перейдите на управляющий узел (node) — ту машину и рабочую директорию Ansible, с которых выполнялась [установка и развёртывание](../../devops/deployment-options/ansible/initial-deployment/index.md) кластера {{ ydb-short-name }}. Выполните команду:

```bash
ansible-playbook ydb_platform.ydb.prepare_drives -l static-node.ydb-cluster.com 
--extra-vars "ydb_disk_prepare=ydb_disk_2"
```

Замените значения параметров:

- `static-node.ydb-cluster.com` на адрес узла (node), на котором выполняется замена диска;

- в аргументе `ydb_disk_prepare` значение `ydb_disk_2` — на фактический `label` заменяемого диска.

### Обновите конфигурацию на узлах

```bash
ansible-playbook ydb_platform.ydb.update_config
```

Результат работы команды:

```bash
PLAY RECAP *************************************************************************************************************
static-node-1.ydb-cluster.com : ok=46   changed=6    unreachable=0    failed=0    skipped=46   rescued=0    ignored=0
static-node-2.ydb-cluster.com : ok=33   changed=6    unreachable=0    failed=0    skipped=18   rescued=0    ignored=0
static-node-3.ydb-cluster.com : ok=33   changed=6    unreachable=0    failed=0    skipped=18   rescued=0    ignored=0
```

{% cut "Индикация состояния замененного диска зеленого цвета" %}

После подготовки диска и `update_config` в том же разделе **PDisks** диск с новым разделом должен отображаться с зелёной индикацией — признак того, что узел видит диск и он участвует в хранилище без ошибок на уровне мониторинга.

![_](_assets/step-2.png)

![_](_assets/step-2-1.png)

{% endcut %}
