# Работа с {{ ydb-short-name }} с помощью Jupyter Notebook

[Jupyter Notebook](https://jupyter.org) - это инструмент с открытым исходным кодом для создания общедоступных документов, сочетающий в себе код, описания на простом языке, данные, богатую визуализацию и интерактивные элементы управления.

[Диалект ydb-sqlalchemy](https://github.com/ydb-platform/ydb-sqlalchemy/releases) позволяет работать с {{ ydb-short-name }} напрямую из таких инструментов как:

* [Pandas](https://pandas.pydata.org/)
* [JupySQL](https://jupysql.ploomber.io/)

## Пример

Подробный пример работы доступен в специальном [ноутбуке](https://github.com/ydb-platform/ydb-sqlalchemy/blob/main/examples/jupyter_notebook/YDB%20SQLAlchemy%20%2B%20Jupyter%20Notebook%20Example.ipynb).

Требования для запуска:

1. Python 3.8+
2. [Jupyter Notebook](https://jupyter.org/install#jupyter-notebook)
3. Существующий кластер {{ ydb-short-name }}, однонодовая инсталляция из [быстрого старта](../../quickstart.md) будет достаточной

Для запуска примера скачайте файл ноутбука <!-- markdownlint-disable no-bare-urls -->{% file src="https://raw.githubusercontent.com/ydb-platform/ydb-sqlalchemy/refs/heads/main/examples/jupyter_notebook/YDB%20SQLAlchemy%20%2B%20Jupyter%20Notebook%20Example.ipynb" name="YDB SQLAlchemy - Jupyter Notebook Example.ipynb" %}, откройте его в Jupyter и последовательно пройдитесь по каждой ячейке, выполняя код по мере необходимости.