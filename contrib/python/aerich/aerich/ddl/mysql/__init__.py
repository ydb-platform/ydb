from __future__ import annotations

from typing import TYPE_CHECKING

from tortoise.backends.mysql.schema_generator import MySQLSchemaGenerator

from aerich.ddl import BaseDDL

if TYPE_CHECKING:
    from tortoise import Model


class MysqlDDL(BaseDDL):
    schema_generator_cls = MySQLSchemaGenerator
    DIALECT = MySQLSchemaGenerator.DIALECT
    _DROP_TABLE_TEMPLATE = "DROP TABLE IF EXISTS `{table_name}`"
    _ADD_COLUMN_TEMPLATE = "ALTER TABLE `{table_name}` ADD {column}"
    _ALTER_DEFAULT_TEMPLATE = "ALTER TABLE `{table_name}` ALTER COLUMN `{column}` {default}"
    _CHANGE_COLUMN_TEMPLATE = (
        "ALTER TABLE `{table_name}` CHANGE {old_column_name} {new_column_name} {new_column_type}"
    )
    _DROP_COLUMN_TEMPLATE = "ALTER TABLE `{table_name}` DROP COLUMN `{column_name}`"
    _RENAME_COLUMN_TEMPLATE = (
        "ALTER TABLE `{table_name}` RENAME COLUMN `{old_column_name}` TO `{new_column_name}`"
    )
    _ADD_INDEX_TEMPLATE = "ALTER TABLE `{table_name}` ADD {index_type}{unique}INDEX `{index_name}` ({column_names}){extra}"
    _DROP_INDEX_TEMPLATE = "ALTER TABLE `{table_name}` DROP INDEX `{index_name}`"
    _ADD_INDEXED_UNIQUE_TEMPLATE = (
        "ALTER TABLE `{table_name}` DROP INDEX `{index_name}`, ADD UNIQUE (`{column_name}`)"
    )
    _DROP_INDEXED_UNIQUE_TEMPLATE = "ALTER TABLE `{table_name}` DROP INDEX `{column_name}`, ADD INDEX `{index_name}` (`{column_name}`)"
    _ADD_FK_TEMPLATE = "ALTER TABLE `{table_name}` ADD CONSTRAINT `{fk_name}` FOREIGN KEY (`{db_column}`) REFERENCES `{table}` (`{field}`) ON DELETE {on_delete}"
    _DROP_FK_TEMPLATE = "ALTER TABLE `{table_name}` DROP FOREIGN KEY `{fk_name}`"
    _M2M_TABLE_TEMPLATE = (
        "CREATE TABLE `{table_name}` (\n"
        "    `{backward_key}` {backward_type} NOT NULL REFERENCES `{backward_table}` (`{backward_field}`) ON DELETE CASCADE,\n"
        "    `{forward_key}` {forward_type} NOT NULL REFERENCES `{forward_table}` (`{forward_field}`) ON DELETE CASCADE\n"
        "){extra}{comment}"
    )
    _MODIFY_COLUMN_TEMPLATE = "ALTER TABLE `{table_name}` MODIFY COLUMN {column}"
    _RENAME_TABLE_TEMPLATE = "ALTER TABLE `{old_table_name}` RENAME TO `{new_table_name}`"

    def _index_name(self, unique: bool | None, model: type[Model], field_names: list[str]) -> str:
        if unique and len(field_names) == 1:
            # Example: `email = CharField(max_length=50, unique=True)`
            # Generate schema: `"email" VARCHAR(10) NOT NULL UNIQUE`
            # Unique index key is the same as field name: `email`
            return field_names[0]
        return super()._index_name(unique, model, field_names)

    def alter_indexed_column_unique(
        self, model: type[Model], field_name: str, drop: bool = False
    ) -> list[str]:
        # if drop is false: Drop index and add unique
        # else: Drop unique index and add normal index
        template = self._DROP_INDEXED_UNIQUE_TEMPLATE if drop else self._ADD_INDEXED_UNIQUE_TEMPLATE
        table = self.get_table_name(model)
        index = self._index_name(unique=False, model=model, field_names=[field_name])
        return [template.format(table_name=table, index_name=index, column_name=field_name)]
