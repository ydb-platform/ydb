import inspect
from .lib import AbcMapping
from .dataerror import DataError
from .lib import (
    _empty,
)
from . import codes


class TrafaretAsyncMixin:
    async def async_check(self, value, context=None):
        if hasattr(self, 'async_transform'):
            return (await self.async_transform(value, context=context))
        return self.check(value, context=context)


class OrAsyncMixin:
    async def async_transform(self, value, context=None):
        errors = []
        for trafaret in self.trafarets:
            try:
                return (await trafaret.async_check(value, context=context))
            except DataError as e:
                errors.append(e)
        self._failure(dict(enumerate(errors)), code=codes.NOTHING_MATCH)


class AndAsyncMixin:
    async def async_transform(self, value, context=None):
        res = await self.trafaret.async_check(value, context=context)
        res = await self.other.async_check(res, context=context)
        return res


class ListAsyncMixin:
    async def async_transform(self, value, context=None):
        self.check_common(value)
        lst = []
        errors = {}
        for index, item in enumerate(value):
            try:
                lst.append(await self.trafaret.async_check(item, context=context))
            except DataError as err:
                errors[index] = err
        if errors:
            self._failure(error=errors, code=codes.SOME_ELEMENTS_DID_NOT_MATCH)
        return lst


class TupleAsyncMixin:
    async def async_transform(self, value, context=None):
        self.check_common(value)
        result = []
        errors = {}
        for idx, (item, trafaret) in enumerate(zip(value, self.trafarets)):
            try:
                result.append(await trafaret.async_check(item, context=context))
            except DataError as err:
                errors[idx] = err
        if errors:
            self._failure(errors, value=value, code=codes.SOME_ELEMENTS_DID_NOT_MATCH)
        return tuple(result)


class MappingAsyncMixin:
    async def async_transform(self, mapping, context=None):
        if not isinstance(mapping, AbcMapping):
            self._failure("value is not a dict", value=mapping, code=codes.IS_NOT_A_DICT)
        checked_mapping = {}
        errors = {}
        for key, value in mapping.items():
            pair_errors = {}
            try:
                checked_key = await self.key.async_check(key, context=context)
            except DataError as err:
                pair_errors['key'] = err
            try:
                checked_value = await self.value.async_check(value, context=context)
            except DataError as err:
                pair_errors['value'] = err
            if pair_errors:
                errors[key] = DataError(error=pair_errors, code=codes.PAIR_MEMBERS_DID_NOT_MATCH)
            else:
                checked_mapping[checked_key] = checked_value
        if errors:
            self._failure(error=errors, code=codes.SOME_ELEMENTS_DID_NOT_MATCH)
        return checked_mapping


class CallAsyncMixin:
    async def async_transform(self, value, context=None):
        if not inspect.iscoroutinefunction(self.fn):
            return self.transform(value, context=context)
        if self.supports_context:
            res = await self.fn(value, context=context)
        else:
            res = await self.fn(value)
        if isinstance(res, DataError):
            raise res
        else:
            return res


class ForwardAsyncMixin:
    async def async_transform(self, value, context=None):
        if self.trafaret is None:
            self._failure('trafaret not set yet', value=value, code=codes.TRAFARET_IS_NOT_SET)
        return (await self.trafaret.async_check(value, context=context))


class DictAsyncMixin:
    async def async_transform(self, value, context=None):
        if not isinstance(value, AbcMapping):
            self._failure("value is not a dict", value=value, code=codes.IS_NOT_A_DICT)
        collect = {}
        errors = {}
        touched_names = []
        for key in self._keys:
            key_run = getattr(key, 'async_call', key)(
                value,
                context=context,
            )
            if inspect.isasyncgen(key_run):
                async for k, v, names in key_run:
                    if isinstance(v, DataError):
                        errors[k] = v
                    else:
                        collect[k] = v
                    touched_names.extend(names)
            else:
                for k, v, names in key_run:
                    if isinstance(v, DataError):
                        errors[k] = v
                    else:
                        collect[k] = v
                    touched_names.extend(names)

        if not self.ignore_any:
            for key in value:
                if key in touched_names:
                    continue
                if key in self.ignore:
                    continue
                if not self.allow_any and key not in self.extras:
                    if key in collect:
                        errors[key] = DataError("%s key was shadowed" % key, code=codes.SHADOWED)
                    else:
                        errors[key] = DataError("%s is not allowed key" % key, code=codes.NOT_ALLOWED)
                elif key in collect:
                    errors[key] = DataError("%s key was shadowed" % key, code=codes.SHADOWED)
                else:
                    try:
                        collect[key] = await self.extras_trafaret.async_check(value[key])
                    except DataError as de:
                        errors[key] = de
        if errors:
            self._failure(error=errors, code=codes.SOME_ELEMENTS_DID_NOT_MATCH)
        return collect


class KeyAsyncMixin:
    async def async_call(self, data, context=None):
        if self.name in data or self.default is not _empty:
            if callable(self.default):
                default = self.default()
            else:
                default = self.default
            try:
                value = await self.trafaret.async_check(self.get_data(data, default), context=context)
            except DataError as data_error:
                value = data_error
            yield (
                self.get_name(),
                value,
                (self.name,)
            )
            return

        if not self.optional:
            yield self.name, DataError(error='is required', code=codes.REQUIRED), (self.name,)
