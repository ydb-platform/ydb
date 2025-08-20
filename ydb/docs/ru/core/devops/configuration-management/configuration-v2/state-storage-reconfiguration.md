# Перемещение State Storage

Если нужно изменить конфигурацию [State Storage](../../../reference/configuration/index.md#domains-state), на кластере {{ ydb-short-name }}, в связи с длительным отказом узлов на которых работают реплики, или увеличением размеров кластера и возрастанием нагрузки.

{% include [warning-configuration-error](../configuration-v1/_includes/warning-configuration-error.md) %}

При использовании Конфигурации V2 конфигурирование State Storage осуществляется автоматически. Но во время работы кластера возможны длительные отказы нод, или увеличение нагрузки в связи с чем может потребоваться изменение конфигурации.

Если на кластере необходимо поменять конфигурацию, то это можно сделать через distconf в ручную.

## GetStateStorageConfig

Возвращает текущую конфигурацию для StateStorage, Board, SchemeBoard
	Recommended: true вернет рекоммендуемую конфигурацию для этого кластера - можно сравнить с текущей для принятия решения о необходимости ее применения
	PileupReplicas - создавать рекоммендуемую конфигурацию с учетом возможности отката конфига на V1
Пример запроса
```shell
curl -ks http://{host_name}:8765/actors/nodewarden?page=distconf -X POST -H 'Content-Type: application/json' -d '{"GetStateStorageConfig": {"Recommended": true}}' | jq
```

## SelfHealStateStorage

Команда начать перевод конфигурации к рекоммендуемой. Выполняется в 4 шага, между шагами делается пауза WaitForConfigStep, чтобы новая конфигураия успела распространиться на узлы, примениться, новые реплики создались и наполнились данными.
Пример:
```shell
curl -ks http://{host_name}:8765/actors/nodewarden?page=distconf -X POST -H 'Content-Type: application/json' -d '
{
	"SelfHealStateStorage": {
		"ForceHeal": true
	}
}' | jq
```

**WaitForConfigStep** - Период между шагами изменения конфигурации. По умолчанию 60сек.
**ForceHeal** - Если distconf не может сгенерировать конфиг только из хороших узлов - он будет задействовать нерабочие. Это снижает отказоустойчивость. Применить такой конфиг можно выставив эту опцию в true.
**PileupReplicas** - создавать рекоммендуемую конфигурацию с учетом возможности отката конфига на V1

## ReconfigStateStorage

Команда применить указанную конфигурацию. Отдельно указывается конфигурация StateStorageConfig, StateStorageBoardConfig, SchemeBoardConfig.
Изменять конфигурацию напрямую нельзя - это приведет к отказу кластера. Поэтому изменение производится добавлением новых и удалением старых групп колец.
При этом добавлять и удалять можно только группы колец с включенным флагом WriteOnly: true.
Первая в списке группа колец должна быть WriteOnly: false или не иметь этого флага. Это условие гарантирует что всегда будет хотябы одна полностью рабочая группа колец.
Между шгами применения новой конфигурации рекоммендуется ожидать время (1 мин) пока новая конфигурация распространится по узлам кластера, создадутся и наполнятся новые реплики.
На примере SchemeBoardConfig (для остальных аналогично и можно выполнять одновременно)

**Шаг 1**
На первом шаге первая группа колец должна соответствовать текущей. Добавляем новую группу колец, которая соответствует целевой конфигурации и помечаем ее WriteOnly: true.
```shell
curl -ks http://{host_name}:8765/actors/nodewarden?page=distconf -X POST -H 'Content-Type: application/json' -d '
{
	ReconfigStateStorage: {
		SchemeBoardConfig: {
			RingGroups: [
				{ NToSelect: 5, Node: [1,2,3,4,5,6,7,8] },
				{ NToSelect: 5, Node: [10,20,30,40,5,6,7,8], WriteOnly: true }
			]
		}
	}
}
```

**Шаг 2**
Снимаем флаг WriteOnly.
```shell
curl -ks http://{host_name}:8765/actors/nodewarden?page=distconf -X POST -H 'Content-Type: application/json' -d '
{
	ReconfigStateStorage: {
		SchemeBoardConfig: {
			RingGroups: [
				{ NToSelect: 5, Node: [1,2,3,4,5,6,7,8] },
				{ NToSelect: 5, Node: [10,20,30,40,5,6,7,8] }
			]
		}
	}
}
```

**Шаг 3**
Делаем новую группу колец основной. Старую конфигурацию готовим к удалению выставляя флаг WriteOnly: true
```shell
curl -ks http://{host_name}:8765/actors/nodewarden?page=distconf -X POST -H 'Content-Type: application/json' -d '
{
	ReconfigStateStorage: {
		SchemeBoardConfig: {
			RingGroups: [
				{ NToSelect: 5, Node: [10,20,30,40,5,6,7,8] },
				{ NToSelect: 5, Node: [1,2,3,4,5,6,7,8], WriteOnly: true }
			]
		}
	}
}
```

**Шаг 4**
Остается одна новая конфигурация.
```shell
curl -ks http://{host_name}:8765/actors/nodewarden?page=distconf -X POST -H 'Content-Type: application/json' -d '
{
	ReconfigStateStorage: {
		SchemeBoardConfig: {
			RingGroups: [
				{ NToSelect: 5, Node: [10,20,30,40,5,6,7,8] }
			]
		}
	}
}
```
