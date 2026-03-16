Introduction
============

This project is simple ORM for working with the [ClickHouse database](https://clickhouse.yandex/).
It allows you to define model classes whose instances can be written to the database and read from it.

Let's jump right in with a simple example of monitoring CPU usage. First we need to define the model class,
connect to the database and create a table for the model:

```python
from infi.clickhouse_orm import Database, Model, DateTimeField, UInt16Field, Float32Field, Memory, F

class CPUStats(Model):

    timestamp = DateTimeField()
    cpu_id = UInt16Field()
    cpu_percent = Float32Field()

    engine = Memory()

db = Database('demo')
db.create_table(CPUStats)
```

Now we can collect usage statistics per CPU, and write them to the database:

```python
import psutil, time, datetime

psutil.cpu_percent(percpu=True) # first sample should be discarded
while True:
    time.sleep(1)
    stats = psutil.cpu_percent(percpu=True)
    timestamp = datetime.datetime.now()
    db.insert([
        CPUStats(timestamp=timestamp, cpu_id=cpu_id, cpu_percent=cpu_percent)
        for cpu_id, cpu_percent in enumerate(stats)
    ])
```

Querying the table is easy, using either the query builder or raw SQL:

```python
# Calculate what percentage of the time CPU 1 was over 95% busy
queryset = CPUStats.objects_in(db)
total = queryset.filter(CPUStats.cpu_id == 1).count()
busy = queryset.filter(CPUStats.cpu_id == 1, CPUStats.cpu_percent > 95).count()
print('CPU 1 was busy {:.2f}% of the time'.format(busy * 100.0 / total))

# Calculate the average usage per CPU
for row in queryset.aggregate(CPUStats.cpu_id, average=F.avg(CPUStats.cpu_percent)):
    print('CPU {row.cpu_id}: {row.average:.2f}%'.format(row=row))
```

This and other examples can be found in the `examples` folder.

To learn more please visit the [documentation](docs/toc.md).
