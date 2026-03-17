from __future__ import annotations

from typing import TYPE_CHECKING, Any

import anyio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session, selectinload
from sqlalchemy.sql.expression import Select, and_, or_
from starlette.requests import Request

from sqladmin._types import MODEL_PROPERTY
from sqladmin.helpers import (
    get_column_python_type,
    get_direction,
    get_primary_keys,
    is_falsy_value,
    object_identifier_values,
)

if TYPE_CHECKING:
    from sqladmin.models import ModelView


class Query:
    def __init__(self, model_view: "ModelView") -> None:
        self.model_view = model_view

    def _get_to_many_stmt(self, relation: MODEL_PROPERTY, values: list[Any]) -> Select:
        target = relation.mapper.class_

        target_pks = get_primary_keys(target)

        if len(target_pks) == 1:
            target_pk = target_pks[0]
            target_pk_type = get_column_python_type(target_pk)
            pk_values = [target_pk_type(value) for value in values]
            return select(target).where(target_pk.in_(pk_values))

        conditions = []
        for value in values:
            conditions.append(
                and_(  # type: ignore[type-var]
                    *(
                        pk == value
                        for pk, value in zip(
                            target_pks,
                            object_identifier_values(value, target),
                        )
                    )
                )
            )
        return select(target).where(or_(*conditions))

    def _get_to_one_stmt(self, relation: MODEL_PROPERTY, value: Any) -> Select:
        target = relation.mapper.class_
        target_pks = get_primary_keys(target)
        target_pk_types = [get_column_python_type(pk) for pk in target_pks]
        conditions = [pk == typ(value) for pk, typ in zip(target_pks, target_pk_types)]
        related_stmt = select(target).where(*conditions)
        return related_stmt

    def _set_many_to_one(self, obj: Any, relation: MODEL_PROPERTY, ident: Any) -> Any:
        values = object_identifier_values(ident, relation.entity)
        pks = get_primary_keys(relation.entity)

        # ``relation.local_remote_pairs`` is ordered by the foreign keys
        # but the values are ordered by the primary keys. This dict
        # ensures we write the correct value to the fk fields
        pk_value = dict(zip(pks, values))

        for fk, pk in relation.local_remote_pairs or []:
            setattr(obj, fk.name, pk_value[pk])  # type: ignore[index]

        return obj

    def _set_attributes_sync(self, session: Session, obj: Any, data: dict) -> Any:
        for key, value in data.items():
            column = self.model_view._mapper.columns.get(key)
            relation = self.model_view._mapper.relationships.get(key)

            # Set falsy values to None, if column is Nullable
            if not value:
                if is_falsy_value(value) and not relation and column.nullable:
                    value = None
                setattr(obj, key, value)
                continue

            if relation:
                direction = get_direction(relation)
                if direction in ["ONETOMANY", "MANYTOMANY"]:
                    related_stmt = self._get_to_many_stmt(relation, value)
                    related_objs = session.execute(related_stmt).scalars().all()
                    setattr(obj, key, related_objs)
                elif direction == "ONETOONE":
                    related_stmt = self._get_to_one_stmt(relation, value)
                    related_obj = session.execute(related_stmt).scalars().first()
                    setattr(obj, key, related_obj)
                else:
                    obj = self._set_many_to_one(obj, relation, value)
            else:
                setattr(obj, key, value)

        return obj

    async def _set_attributes_async(
        self, session: AsyncSession, obj: Any, data: dict
    ) -> Any:
        for key, value in data.items():
            column = self.model_view._mapper.columns.get(key)
            relation = self.model_view._mapper.relationships.get(key)

            # Set falsy values to None, if column is Nullable
            if not value:
                if is_falsy_value(value) and not relation and column.nullable:
                    value = None
                setattr(obj, key, value)
                continue

            if relation:
                direction = get_direction(relation)
                if direction in ["ONETOMANY", "MANYTOMANY"]:
                    related_stmt = self._get_to_many_stmt(relation, value)
                    result = await session.execute(related_stmt)
                    related_objs = result.scalars().all()
                    setattr(obj, key, related_objs)
                elif direction == "ONETOONE":
                    related_stmt = self._get_to_one_stmt(relation, value)
                    result = await session.execute(related_stmt)
                    related_obj = result.scalars().first()
                    setattr(obj, key, related_obj)
                else:
                    obj = self._set_many_to_one(obj, relation, value)
            else:
                setattr(obj, key, value)
        return obj

    def _update_sync(self, pk: Any, data: dict[str, Any], request: Request) -> Any:
        stmt = self.model_view._stmt_by_identifier(pk)

        with self.model_view.session_maker(expire_on_commit=False) as session:
            obj = session.execute(stmt).scalars().first()
            anyio.from_thread.run(
                self.model_view.on_model_change, data, obj, False, request
            )
            obj = self._set_attributes_sync(session, obj, data)
            session.commit()
            anyio.from_thread.run(
                self.model_view.after_model_change, data, obj, False, request
            )
            return obj

    async def _update_async(
        self, pk: Any, data: dict[str, Any], request: Request
    ) -> Any:
        stmt = self.model_view._stmt_by_identifier(pk)

        for relation in self.model_view._form_relations:
            stmt = stmt.options(selectinload(relation))

        async with self.model_view.session_maker(expire_on_commit=False) as session:
            result = await session.execute(stmt)
            obj = result.scalars().first()
            await self.model_view.on_model_change(data, obj, False, request)
            obj = await self._set_attributes_async(session, obj, data)
            await session.commit()
            await self.model_view.after_model_change(data, obj, False, request)
            return obj

    def _get_delete_stmt(self, pk: str) -> Select:
        stmt = select(self.model_view.model)
        pks = get_primary_keys(self.model_view.model)
        values = object_identifier_values(pk, self.model_view.model)
        conditions = [pk == value for (pk, value) in zip(pks, values)]
        return stmt.where(*conditions)

    def _delete_sync(self, pk: str, request: Request) -> None:
        with self.model_view.session_maker() as session:
            obj = session.execute(self._get_delete_stmt(pk)).scalar_one_or_none()
            anyio.from_thread.run(self.model_view.on_model_delete, obj, request)
            session.delete(obj)
            session.commit()
            anyio.from_thread.run(self.model_view.after_model_delete, obj, request)

    async def _delete_async(self, pk: str, request: Request) -> None:
        async with self.model_view.session_maker() as session:
            result = await session.execute(self._get_delete_stmt(pk))
            obj = result.scalars().first()
            await self.model_view.on_model_delete(obj, request)
            await session.delete(obj)
            await session.commit()
            await self.model_view.after_model_delete(obj, request)

    def _insert_sync(self, data: dict[str, Any], request: Request) -> Any:
        obj = self.model_view.model()

        with self.model_view.session_maker(expire_on_commit=False) as session:
            anyio.from_thread.run(
                self.model_view.on_model_change, data, obj, True, request
            )
            obj = self._set_attributes_sync(session, obj, data)
            session.add(obj)
            session.commit()
            anyio.from_thread.run(
                self.model_view.after_model_change, data, obj, True, request
            )
            return obj

    async def _insert_async(self, data: dict[str, Any], request: Request) -> Any:
        obj = self.model_view.model()

        async with self.model_view.session_maker(expire_on_commit=False) as session:
            await self.model_view.on_model_change(data, obj, True, request)
            obj = await self._set_attributes_async(session, obj, data)
            session.add(obj)
            await session.commit()
            await self.model_view.after_model_change(data, obj, True, request)
            return obj

    async def delete(self, obj: Any, request: Request) -> None:
        if self.model_view.is_async:
            coro = self._delete_async(obj, request)
        else:
            coro = anyio.to_thread.run_sync(self._delete_sync, obj, request)

        return await coro

    async def insert(self, data: dict, request: Request) -> Any:
        if self.model_view.is_async:
            coro = self._insert_async(data, request)
        else:
            coro = anyio.to_thread.run_sync(self._insert_sync, data, request)

        return await coro

    async def update(self, pk: Any, data: dict, request: Request) -> Any:
        if self.model_view.is_async:
            coro = self._update_async(pk, data, request)
        else:
            coro = anyio.to_thread.run_sync(self._update_sync, pk, data, request)

        return await coro
