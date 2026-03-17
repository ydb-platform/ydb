from __future__ import division
import collections
from collections import OrderedDict
try:
    from collections.abc import Iterable, Mapping, MutableMapping
except ImportError:
    from collections import Iterable, Mapping, MutableMapping
import copy
import functools
import itertools
import json
import math
import time
import warnings

try:
    from bson import json_util, SON, BSON
except ImportError:
    json_utils = SON = BSON = None
try:
    import execjs
except ImportError:
    execjs = None

try:
    from pymongo.operations import IndexModel
    from pymongo import ReadPreference
    from pymongo import ReturnDocument
    _READ_PREFERENCE_PRIMARY = ReadPreference.PRIMARY
except ImportError:
    class IndexModel(object):
        pass

    class ReturnDocument(object):
        BEFORE = False
        AFTER = True

    from mongomock.read_preferences import PRIMARY as _READ_PREFERENCE_PRIMARY

from sentinels import NOTHING
from six import iteritems
from six import iterkeys
from six import raise_from
from six import string_types
from six import text_type


import mongomock  # Used for utcnow - please see https://github.com/mongomock/mongomock#utcnow
from mongomock import aggregate
from mongomock import codec_options as mongomock_codec_options
from mongomock import ConfigurationError, DuplicateKeyError, BulkWriteError
from mongomock import filtering
from mongomock.filtering import filter_applies
from mongomock import helpers
from mongomock import InvalidOperation
from mongomock.not_implemented import raise_for_feature as raise_not_implemented
from mongomock import ObjectId
from mongomock import OperationFailure
from mongomock.results import BulkWriteResult
from mongomock.results import DeleteResult
from mongomock.results import InsertManyResult
from mongomock.results import InsertOneResult
from mongomock.results import UpdateResult
from mongomock.write_concern import WriteConcern
from mongomock import WriteError

try:
    from pymongo.read_concern import ReadConcern
except ImportError:
    from mongomock.read_concern import ReadConcern

if hasattr(time, 'perf_counter'):
    _get_perf_counter = time.perf_counter
else:
    _get_perf_counter = time.clock

_KwargOption = collections.namedtuple('KwargOption', ['typename', 'default', 'attrs'])

_WITH_OPTIONS_KWARGS = {
    'read_preference': _KwargOption(
        'pymongo.read_preference.ReadPreference', _READ_PREFERENCE_PRIMARY,
        ('document', 'mode', 'mongos_mode', 'max_staleness')),
    'write_concern': _KwargOption(
        'pymongo.write_concern.WriteConcern', WriteConcern(),
        ('acknowledged', 'document')),
}


def validate_is_mapping(option, value):
    if not isinstance(value, Mapping):
        raise TypeError('%s must be an instance of dict, bson.son.SON, or '
                        'other type that inherits from '
                        'collections.Mapping' % (option,))


def validate_is_mutable_mapping(option, value):
    if not isinstance(value, MutableMapping):
        raise TypeError('%s must be an instance of dict, bson.son.SON, or '
                        'other type that inherits from '
                        'collections.MutableMapping' % (option,))


def validate_ok_for_replace(replacement):
    validate_is_mapping('replacement', replacement)
    if replacement:
        first = next(iter(replacement))
        if first.startswith('$'):
            raise ValueError('replacement can not include $ operators')


def validate_ok_for_update(update):
    validate_is_mapping('update', update)
    if not update:
        raise ValueError('update only works with $ operators')
    first = next(iter(update))
    if not first.startswith('$'):
        raise ValueError('update only works with $ operators')


def validate_write_concern_params(**params):
    if params:
        WriteConcern(**params)


class BulkWriteOperation(object):
    def __init__(self, builder, selector, is_upsert=False):
        self.builder = builder
        self.selector = selector
        self.is_upsert = is_upsert

    def upsert(self):
        assert not self.is_upsert
        return BulkWriteOperation(self.builder, self.selector, is_upsert=True)

    def register_remove_op(self, multi, hint=None):
        collection = self.builder.collection
        selector = self.selector

        def exec_remove():
            if multi:
                op_result = collection.delete_many(selector, hint=hint).raw_result
            else:
                op_result = collection.delete_one(selector, hint=hint).raw_result
            if op_result.get('ok'):
                return {'nRemoved': op_result.get('n')}
            err = op_result.get('err')
            if err:
                return {'writeErrors': [err]}
            return {}
        self.builder.executors.append(exec_remove)

    def remove(self):
        assert not self.is_upsert
        self.register_remove_op(multi=True)

    def remove_one(self,):
        assert not self.is_upsert
        self.register_remove_op(multi=False)

    def register_update_op(self, document, multi, **extra_args):
        if not extra_args.get('remove'):
            validate_ok_for_update(document)

        collection = self.builder.collection
        selector = self.selector

        def exec_update():
            result = collection._update(spec=selector, document=document,
                                        multi=multi, upsert=self.is_upsert,
                                        **extra_args)
            ret_val = {}
            if result.get('upserted'):
                ret_val['upserted'] = result.get('upserted')
                ret_val['nUpserted'] = result.get('n')
            else:
                matched = result.get('n')
                if matched is not None:
                    ret_val['nMatched'] = matched
            modified = result.get('nModified')
            if modified is not None:
                ret_val['nModified'] = modified
            if result.get('err'):
                ret_val['err'] = result.get('err')
            return ret_val
        self.builder.executors.append(exec_update)

    def update(self, document, hint=None):
        self.register_update_op(document, multi=True, hint=hint)

    def update_one(self, document, hint=None):
        self.register_update_op(document, multi=False, hint=hint)

    def replace_one(self, document, hint=None):
        self.register_update_op(document, multi=False, remove=True, hint=hint)


def _combine_projection_spec(projection_fields_spec):
    """Re-format a projection fields spec into a nested dictionary.

    e.g: {'a': 1, 'b.c': 1, 'b.d': 1} => {'a': 1, 'b': {'c': 1, 'd': 1}}
    """

    tmp_spec = OrderedDict()
    for f, v in iteritems(projection_fields_spec):
        if '.' not in f:
            if isinstance(tmp_spec.get(f), dict):
                if not v:
                    raise NotImplementedError(
                        'Mongomock does not support overriding excluding projection: %s' %
                        projection_fields_spec)
                raise OperationFailure('Path collision at %s' % f)
            tmp_spec[f] = v
        else:
            split_field = f.split('.', 1)
            base_field, new_field = tuple(split_field)
            if not isinstance(tmp_spec.get(base_field), dict):
                if base_field in tmp_spec:
                    raise OperationFailure(
                        'Path collision at %s remaining portion %s' % (f, new_field))
                tmp_spec[base_field] = OrderedDict()
            tmp_spec[base_field][new_field] = v

    combined_spec = OrderedDict()
    for f, v in iteritems(tmp_spec):
        if isinstance(v, dict):
            combined_spec[f] = _combine_projection_spec(v)
        else:
            combined_spec[f] = v

    return combined_spec


def _project_by_spec(doc, combined_projection_spec, is_include, container):
    doc_copy = container()

    if not is_include:
        for key, val in iteritems(doc):
            doc_copy[key] = val

    for key, spec in iteritems(combined_projection_spec):
        if key == '$':
            if is_include:
                raise NotImplementedError('Positional projection is not implemented in mongomock')
            raise OperationFailure('Cannot exclude array elements with the positional operator')
        if key not in doc:
            continue

        if isinstance(spec, dict):
            sub = doc[key]
            if isinstance(sub, (list, tuple)):
                doc_copy[key] = [_project_by_spec(sub_doc, spec, is_include, container)
                                 for sub_doc in sub]
            elif isinstance(sub, dict):
                doc_copy[key] = _project_by_spec(sub, spec, is_include, container)
        else:
            if is_include:
                doc_copy[key] = doc[key]
            else:
                doc_copy.pop(key, None)

    return doc_copy


class BulkOperationBuilder(object):
    def __init__(self, collection, ordered=False, bypass_document_validation=False):
        self.collection = collection
        self.ordered = ordered
        self.results = {}
        self.executors = []
        self.done = False
        self._insert_returns_nModified = True
        self._update_returns_nModified = True
        self._bypass_document_validation = bypass_document_validation

    def find(self, selector):
        return BulkWriteOperation(self, selector)

    def insert(self, doc):
        def exec_insert():
            self.collection.insert_one(
                doc, bypass_document_validation=self._bypass_document_validation)
            return {'nInserted': 1}
        self.executors.append(exec_insert)

    def __aggregate_operation_result(self, total_result, key, value):
        agg_val = total_result.get(key)
        assert agg_val is not None, 'Unknow operation result %s=%s' \
                                    ' (unrecognized key)' % (key, value)
        if isinstance(agg_val, int):
            total_result[key] += value
        elif isinstance(agg_val, list):
            if key == 'upserted':
                new_element = {'index': len(agg_val), '_id': value}
                agg_val.append(new_element)
            else:
                agg_val.append(value)
        else:
            assert False, 'Fixme: missed aggreation rule for type: %s for' \
                          ' key {%s=%s}' % (type(agg_val), key, agg_val)

    def _set_nModified_policy(self, insert, update):
        self._insert_returns_nModified = insert
        self._update_returns_nModified = update

    def execute(self, write_concern=None):
        if not self.executors:
            raise InvalidOperation('Bulk operation empty!')
        if self.done:
            raise InvalidOperation('Bulk operation already executed!')
        self.done = True
        result = {'nModified': 0, 'nUpserted': 0, 'nMatched': 0,
                  'writeErrors': [], 'upserted': [], 'writeConcernErrors': [],
                  'nRemoved': 0, 'nInserted': 0}

        has_update = False
        has_insert = False
        broken_nModified_info = False
        for index, execute_func in enumerate(self.executors):
            exec_name = execute_func.__name__
            try:
                op_result = execute_func()
            except WriteError as error:
                result['writeErrors'].append({
                    'index': index,
                    'code': error.code,
                    'errmsg': str(error),
                })
                if self.ordered:
                    break
                continue
            for (key, value) in op_result.items():
                self.__aggregate_operation_result(result, key, value)
            if exec_name == 'exec_update':
                has_update = True
                if 'nModified' not in op_result:
                    broken_nModified_info = True
            has_insert |= exec_name == 'exec_insert'

        if broken_nModified_info:
            result.pop('nModified')
        elif has_insert and self._insert_returns_nModified:
            pass
        elif has_update and self._update_returns_nModified:
            pass
        elif self._update_returns_nModified and self._insert_returns_nModified:
            pass
        else:
            result.pop('nModified')

        if result.get('writeErrors'):
            raise BulkWriteError(result)

        return result

    def add_insert(self, doc):
        self.insert(doc)

    def add_update(self, selector, doc, multi=False, upsert=False, collation=None,
                   array_filters=None, hint=None):
        if array_filters:
            raise NotImplementedError(
                'Array filters are not implemented in mongomock yet.')
        write_operation = BulkWriteOperation(self, selector, is_upsert=upsert)
        write_operation.register_update_op(doc, multi, hint=hint)

    def add_replace(self, selector, doc, upsert, collation=None, hint=None):
        write_operation = BulkWriteOperation(self, selector, is_upsert=upsert)
        write_operation.replace_one(doc, hint=hint)

    def add_delete(self, selector, just_one, collation=None, hint=None):
        write_operation = BulkWriteOperation(self, selector, is_upsert=False)
        write_operation.register_remove_op(not just_one, hint=hint)


class Collection(object):

    def __init__(
            self, database, name, _db_store, write_concern=None, read_concern=None,
            read_preference=None, codec_options=None):
        self.database = database
        self._name = name
        self._db_store = _db_store
        self._write_concern = write_concern or WriteConcern()
        if read_concern and not isinstance(read_concern, ReadConcern):
            raise TypeError('read_concern must be an instance of pymongo.read_concern.ReadConcern')
        self._read_concern = read_concern or ReadConcern()
        self._read_preference = read_preference or _READ_PREFERENCE_PRIMARY
        self._codec_options = codec_options or mongomock_codec_options.CodecOptions()

    def __repr__(self):
        return "Collection({0}, '{1}')".format(self.database, self.name)

    def __getitem__(self, name):
        return self.database[self.name + '.' + name]

    def __getattr__(self, attr):
        if attr.startswith('_'):
            raise AttributeError(
                "%s has no attribute '%s'. To access the %s.%s collection, use database['%s.%s']." %
                (self.__class__.__name__, attr, self.name, attr, self.name, attr))
        return self.__getitem__(attr)

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.database == other.database and self.name == other.name
        return NotImplemented

    @property
    def full_name(self):
        return '{0}.{1}'.format(self.database.name, self._name)

    @property
    def name(self):
        return self._name

    @property
    def write_concern(self):
        return self._write_concern

    @property
    def read_concern(self):
        return self._read_concern

    @property
    def read_preference(self):
        return self._read_preference

    @property
    def codec_options(self):
        return self._codec_options

    def initialize_unordered_bulk_op(self, bypass_document_validation=False):
        return BulkOperationBuilder(
            self, ordered=False, bypass_document_validation=bypass_document_validation)

    def initialize_ordered_bulk_op(self, bypass_document_validation=False):
        return BulkOperationBuilder(
            self, ordered=True, bypass_document_validation=bypass_document_validation)

    def insert(self, data, manipulate=True, check_keys=True,
               continue_on_error=False, **kwargs):
        warnings.warn('insert is deprecated. Use insert_one or insert_many '
                      'instead.', DeprecationWarning, stacklevel=2)
        validate_write_concern_params(**kwargs)
        return self._insert(data)

    def insert_one(self, document, bypass_document_validation=False, session=None):
        if not bypass_document_validation:
            validate_is_mutable_mapping('document', document)
        return InsertOneResult(self._insert(document, session), acknowledged=True)

    def insert_many(self, documents, ordered=True, bypass_document_validation=False, session=None):
        if not isinstance(documents, Iterable) or not documents:
            raise TypeError('documents must be a non-empty list')
        documents = list(documents)
        if not bypass_document_validation:
            for document in documents:
                validate_is_mutable_mapping('document', document)
        return InsertManyResult(
            self._insert(documents, session, ordered=ordered),
            acknowledged=True)

    @property
    def _store(self):
        return self._db_store[self._name]

    def _insert(self, data, session=None, ordered=True):
        if session:
            raise_not_implemented('session', 'Mongomock does not handle sessions yet')
        if not isinstance(data, Mapping):
            results = []
            write_errors = []
            num_inserted = 0
            for index, item in enumerate(data):
                try:
                    results.append(self._insert(item))
                except WriteError as error:
                    write_errors.append({
                        'index': index,
                        'code': error.code,
                        'errmsg': str(error),
                        'op': item,
                    })
                    if ordered:
                        break
                    else:
                        continue
                num_inserted += 1
            if write_errors:
                raise BulkWriteError({
                    'writeErrors': write_errors,
                    'nInserted': num_inserted,
                })
            return results

        if not all(isinstance(k, string_types) for k in data):
            raise ValueError('Document keys must be strings')

        if BSON:
            # bson validation
            BSON.encode(data, check_keys=True)

        # Like pymongo, we should fill the _id in the inserted dict (odd behavior,
        # but we need to stick to it), so we must patch in-place the data dict
        if '_id' not in data:
            data['_id'] = ObjectId()

        object_id = data['_id']
        if isinstance(object_id, dict):
            object_id = helpers.hashdict(object_id)
        if object_id in self._store:
            raise DuplicateKeyError('E11000 Duplicate Key Error', 11000)

        data = helpers.patch_datetime_awareness_in_document(data)

        self._store[object_id] = data
        try:
            self._ensure_uniques(data)
        except DuplicateKeyError:
            # Rollback
            del self._store[object_id]
            raise
        return data['_id']

    def _ensure_uniques(self, new_data):
        # Note we consider new_data is already inserted in db
        for index in self._store.indexes.values():
            if not index.get('unique'):
                continue
            unique = index.get('key')
            is_sparse = index.get('sparse')
            find_kwargs = {}
            for key, _ in unique:
                try:
                    find_kwargs[key] = helpers.get_value_by_dot(new_data, key)
                except KeyError:
                    find_kwargs[key] = None
            if is_sparse and set(find_kwargs.values()) == {None}:
                continue
            answer_count = len(list(self._iter_documents(find_kwargs)))
            if answer_count > 1:
                raise DuplicateKeyError('E11000 Duplicate Key Error', 11000)

    def _internalize_dict(self, d):
        return {k: copy.deepcopy(v) for k, v in iteritems(d)}

    def _has_key(self, doc, key):
        key_parts = key.split('.')
        sub_doc = doc
        for part in key_parts:
            if part not in sub_doc:
                return False
            sub_doc = sub_doc[part]
        return True

    def update_one(
            self, filter, update, upsert=False, bypass_document_validation=False, hint=None,
            session=None, collation=None):
        if not bypass_document_validation:
            validate_ok_for_update(update)
        return UpdateResult(
            self._update(
                filter, update, upsert=upsert, hint=hint, session=session, collation=collation),
            acknowledged=True)

    def update_many(
            self, filter, update, upsert=False, bypass_document_validation=False, hint=None,
            session=None, collation=None):
        if not bypass_document_validation:
            validate_ok_for_update(update)
        return UpdateResult(
            self._update(
                filter, update, upsert=upsert, multi=True, hint=hint, session=session,
                collation=collation),
            acknowledged=True)

    def replace_one(
            self, filter, replacement, upsert=False, bypass_document_validation=False,
            session=None, hint=None):
        if not bypass_document_validation:
            validate_ok_for_replace(replacement)
        return UpdateResult(
            self._update(filter, replacement, upsert=upsert, hint=hint, session=session),
            acknowledged=True)

    def update(self, spec, document, upsert=False, manipulate=False,
               multi=False, check_keys=False, **kwargs):
        warnings.warn('update is deprecated. Use replace_one, update_one or '
                      'update_many instead.', DeprecationWarning, stacklevel=2)
        return self._update(spec, document, upsert, manipulate, multi,
                            check_keys, **kwargs)

    def _update(self, spec, document, upsert=False, manipulate=False,
                multi=False, check_keys=False, hint=None, session=None,
                collation=None, **kwargs):
        if session:
            raise_not_implemented('session', 'Mongomock does not handle sessions yet')
        if hint:
            raise NotImplementedError(
                'The hint argument of update is valid but has not been implemented in '
                'mongomock yet')
        if collation:
            raise_not_implemented(
                'collation',
                'The collation argument of update is valid but has not been implemented in '
                'mongomock yet')
        spec = helpers.patch_datetime_awareness_in_document(spec)
        document = helpers.patch_datetime_awareness_in_document(document)
        validate_is_mapping('spec', spec)
        validate_is_mapping('document', document)
        for operator in _updaters:
            if not document.get(operator, True):
                raise WriteError(
                    "'%s' is empty. You must specify a field like so: {%s: {<field>: ...}}"
                    % (operator, operator),
                )

        updated_existing = False
        upserted_id = None
        num_updated = 0
        num_matched = 0
        for existing_document in itertools.chain(self._iter_documents(spec), [None]):
            # we need was_insert for the setOnInsert update operation
            was_insert = False
            # the sentinel document means we should do an upsert
            if existing_document is None:
                if not upsert or num_matched:
                    continue
                # For upsert operation we have first to create a fake existing_document,
                # update it like a regular one, then finally insert it
                if spec.get('_id') is not None:
                    _id = spec['_id']
                elif document.get('_id') is not None:
                    _id = document['_id']
                else:
                    _id = ObjectId()
                to_insert = dict(spec, _id=_id)
                to_insert = self._expand_dots(to_insert)
                to_insert, _ = self._discard_operators(to_insert)
                existing_document = to_insert
                was_insert = True
            else:
                original_document_snapshot = copy.deepcopy(existing_document)
                updated_existing = True
            num_matched += 1
            first = True
            subdocument = None
            for k, v in iteritems(document):
                if k in _updaters.keys():
                    updater = _updaters[k]
                    subdocument = self._update_document_fields_with_positional_awareness(
                        existing_document, v, spec, updater, subdocument)

                elif k == '$rename':
                    for src, dst in iteritems(v):
                        if '.' in src or '.' in dst:
                            raise NotImplementedError(
                                'Using the $rename operator with dots is a valid MongoDB '
                                'operation, but it is not yet supported by mongomock'
                            )
                        if self._has_key(existing_document, src):
                            existing_document[dst] = existing_document.pop(src)

                elif k == '$setOnInsert':
                    if not was_insert:
                        continue
                    subdocument = self._update_document_fields_with_positional_awareness(
                        existing_document, v, spec, _set_updater, subdocument)

                elif k == '$currentDate':
                    subdocument = self._update_document_fields_with_positional_awareness(
                        existing_document, v, spec, _current_date_updater, subdocument)

                elif k == '$addToSet':
                    for field, value in iteritems(v):
                        nested_field_list = field.rsplit('.')
                        if len(nested_field_list) == 1:
                            if field not in existing_document:
                                existing_document[field] = []
                            # document should be a list append to it
                            if isinstance(value, dict):
                                if '$each' in value:
                                    # append the list to the field
                                    existing_document[field] += [
                                        obj for obj in list(value['$each'])
                                        if obj not in existing_document[field]]
                                    continue
                            if value not in existing_document[field]:
                                existing_document[field].append(value)
                            continue
                        # push to array in a nested attribute
                        else:
                            # create nested attributes if they do not exist
                            subdocument = existing_document
                            for field_part in nested_field_list[:-1]:
                                if field_part not in subdocument:
                                    subdocument[field_part] = {}

                                subdocument = subdocument[field_part]

                            # we're pushing a list
                            push_results = []
                            if nested_field_list[-1] in subdocument:
                                # if the list exists, then use that list
                                push_results = subdocument[
                                    nested_field_list[-1]]

                            if isinstance(value, dict) and '$each' in value:
                                push_results += [
                                    obj for obj in list(value['$each'])
                                    if obj not in push_results]
                            elif value not in push_results:
                                push_results.append(value)

                            subdocument[nested_field_list[-1]] = push_results
                elif k == '$pull':
                    for field, value in iteritems(v):
                        nested_field_list = field.rsplit('.')
                        # nested fields includes a positional element
                        # need to find that element
                        if '$' in nested_field_list:
                            if not subdocument:
                                subdocument, _ = self._get_subdocument(
                                    existing_document, spec, nested_field_list)

                            # value should be a dictionary since we're pulling
                            pull_results = []
                            # and the last subdoc should be an array
                            for obj in subdocument[nested_field_list[-1]]:
                                if isinstance(obj, dict):
                                    for pull_key, pull_value in iteritems(value):
                                        if obj[pull_key] != pull_value:
                                            pull_results.append(obj)
                                    continue
                                if obj != value:
                                    pull_results.append(obj)

                            # cannot write to doc directly as it doesn't save to
                            # existing_document
                            subdocument[nested_field_list[-1]] = pull_results
                        else:
                            arr = existing_document
                            for field_part in nested_field_list:
                                if field_part not in arr:
                                    break
                                arr = arr[field_part]
                            if not isinstance(arr, list):
                                continue

                            arr_copy = copy.deepcopy(arr)
                            if isinstance(value, dict):
                                for obj in arr_copy:
                                    try:
                                        is_matching = filter_applies(value, obj)
                                    except OperationFailure:
                                        is_matching = False
                                    if is_matching:
                                        arr.remove(obj)
                                        continue

                                    if filter_applies({'field': value}, {'field': obj}):
                                        arr.remove(obj)
                            else:
                                for obj in arr_copy:
                                    if value == obj:
                                        arr.remove(obj)
                elif k == '$pullAll':
                    for field, value in iteritems(v):
                        nested_field_list = field.rsplit('.')
                        if len(nested_field_list) == 1:
                            if field in existing_document:
                                arr = existing_document[field]
                                existing_document[field] = [
                                    obj for obj in arr if obj not in value]
                            continue
                        else:
                            subdocument = existing_document
                            for nested_field in nested_field_list[:-1]:
                                if nested_field not in subdocument:
                                    break
                                subdocument = subdocument[nested_field]

                            if nested_field_list[-1] in subdocument:
                                arr = subdocument[nested_field_list[-1]]
                                subdocument[nested_field_list[-1]] = [
                                    obj for obj in arr if obj not in value]
                elif k == '$push':
                    for field, value in iteritems(v):
                        # Find the place where to push.
                        nested_field_list = field.rsplit('.')
                        subdocument, field = self._get_subdocument(
                            existing_document, spec, nested_field_list)

                        # Push the new element or elements.
                        if isinstance(subdocument, dict) and field not in subdocument:
                            subdocument[field] = []
                        push_results = subdocument[field]
                        if isinstance(value, dict) and '$each' in value:
                            if '$position' in value:
                                push_results = \
                                    push_results[0:value['$position']] + \
                                    list(value['$each']) + \
                                    push_results[value['$position']:]
                            else:
                                push_results += list(value['$each'])

                            if '$sort' in value:
                                sort_spec = value['$sort']
                                if isinstance(sort_spec, dict):
                                    sort_key = set(sort_spec.keys()).pop()
                                    push_results = sorted(
                                        push_results,
                                        key=lambda d: helpers.get_value_by_dot(d, sort_key),
                                        reverse=set(sort_spec.values()).pop() < 0)
                                else:
                                    push_results = sorted(push_results, reverse=sort_spec < 0)

                            if '$slice' in value:
                                slice_value = value['$slice']
                                if slice_value < 0:
                                    push_results = push_results[slice_value:]
                                elif slice_value == 0:
                                    push_results = []
                                else:
                                    push_results = push_results[:slice_value]

                            unused_modifiers = \
                                set(value.keys()) - {'$each', '$slice', '$position', '$sort'}
                            if unused_modifiers:
                                raise WriteError(
                                    'Unrecognized clause in $push: ' + unused_modifiers.pop())
                        else:
                            push_results.append(value)
                        subdocument[field] = push_results
                else:
                    if first:
                        # replace entire document
                        for key in document.keys():
                            if key.startswith('$'):
                                # can't mix modifiers with non-modifiers in
                                # update
                                raise ValueError('field names cannot start with $ [{}]'.format(k))
                        _id = spec.get('_id', existing_document.get('_id'))
                        existing_document.clear()
                        if _id is not None:
                            existing_document['_id'] = _id
                        if BSON:
                            # bson validation
                            BSON.encode(document, check_keys=True)
                        existing_document.update(self._internalize_dict(document))
                        if existing_document['_id'] != _id:
                            raise OperationFailure(
                                'The _id field cannot be changed from {0} to {1}'
                                .format(existing_document['_id'], _id))
                        break
                    else:
                        # can't mix modifiers with non-modifiers in update
                        raise ValueError(
                            'Invalid modifier specified: {}'.format(k))
                first = False
            # if empty document comes
            if not document:
                _id = spec.get('_id', existing_document.get('_id'))
                existing_document.clear()
                if _id:
                    existing_document['_id'] = _id

            if was_insert:
                upserted_id = self._insert(existing_document)
                num_updated += 1
            elif existing_document != original_document_snapshot:
                # Document has been modified in-place.

                # Make sure the ID was not change.
                if original_document_snapshot.get('_id') != existing_document.get('_id'):
                    # Rollback.
                    self._store[original_document_snapshot['_id']] = original_document_snapshot
                    raise WriteError(
                        "After applying the update, the (immutable) field '_id' was found to have "
                        'been altered to _id: {}'.format(existing_document.get('_id')))

                # Make sure it still respect the unique indexes and, if not, to
                # revert modifications
                try:
                    self._ensure_uniques(existing_document)
                    num_updated += 1
                except DuplicateKeyError:
                    # Rollback.
                    self._store[original_document_snapshot['_id']] = original_document_snapshot
                    raise

            if not multi:
                break

        return {
            text_type('connectionId'): self.database.client._id,
            text_type('err'): None,
            text_type('n'): num_matched,
            text_type('nModified'): num_updated if updated_existing else 0,
            text_type('ok'): 1,
            text_type('upserted'): upserted_id,
            text_type('updatedExisting'): updated_existing,
        }

    def _get_subdocument(self, existing_document, spec, nested_field_list):
        """This method retrieves the subdocument of the existing_document.nested_field_list.

        It uses the spec to filter through the items. It will continue to grab nested documents
        until it can go no further. It will then return the subdocument that was last saved.
        '$' is the positional operator, so we use the $elemMatch in the spec to find the right
        subdocument in the array.
        """
        # Current document in view.
        doc = existing_document
        # Previous document in view.
        parent_doc = existing_document
        # Current spec in view.
        subspec = spec
        # Whether spec is following the document.
        is_following_spec = True
        # Walk down the dictionary.
        for index, subfield in enumerate(nested_field_list):
            if subfield == '$':
                if not is_following_spec:
                    raise WriteError(
                        'The positional operator did not find the match needed from the query')
                # Positional element should have the equivalent elemMatch in the query.
                subspec = subspec['$elemMatch']
                is_following_spec = False
                # Iterate through.
                for spec_index, item in enumerate(doc):
                    if filter_applies(subspec, item):
                        subfield = spec_index
                        break
                else:
                    raise WriteError(
                        'The positional operator did not find the match needed from the query')

            parent_doc = doc
            if isinstance(parent_doc, list):
                subfield = int(subfield)
                if is_following_spec and (subfield < 0 or subfield >= len(subspec)):
                    is_following_spec = False

            if index == len(nested_field_list) - 1:
                return parent_doc, subfield

            if not isinstance(parent_doc, list):
                if subfield not in parent_doc:
                    parent_doc[subfield] = {}
                if is_following_spec and subfield not in subspec:
                    is_following_spec = False

            doc = parent_doc[subfield]
            if is_following_spec:
                subspec = subspec[subfield]

    def _expand_dots(self, doc):
        expanded = {}
        paths = {}
        for k, v in iteritems(doc):

            def _raise_incompatible(subkey):
                raise WriteError(
                    "cannot infer query fields to set, both paths '%s' and '%s' are matched"
                    % (k, paths[subkey]))

            if k in paths:
                _raise_incompatible(k)

            key_parts = k.split('.')
            sub_expanded = expanded

            paths[k] = k
            for i, key_part in enumerate(key_parts[:-1]):
                if key_part not in sub_expanded:
                    sub_expanded[key_part] = {}
                sub_expanded = sub_expanded[key_part]
                key = '.'.join(key_parts[:i + 1])
                if not isinstance(sub_expanded, dict):
                    _raise_incompatible(key)
                paths[key] = k
            sub_expanded[key_parts[-1]] = v
        return expanded

    def _discard_operators(self, doc):
        if not doc or not isinstance(doc, dict):
            return doc, False
        new_doc = OrderedDict()
        for k, v in iteritems(doc):
            if k == '$eq':
                return v, False
            if k.startswith('$'):
                continue
            new_v, discarded = self._discard_operators(v)
            if not discarded:
                new_doc[k] = new_v
        return new_doc, not bool(new_doc)

    def find(self, filter=None, projection=None, skip=0, limit=0,
             no_cursor_timeout=False, cursor_type=None, sort=None,
             allow_partial_results=False, oplog_replay=False, modifiers=None,
             batch_size=0, manipulate=True, collation=None, session=None,
             max_time_ms=None, **kwargs):
        spec = filter
        if spec is None:
            spec = {}
        validate_is_mapping('filter', spec)
        for kwarg, value in iteritems(kwargs):
            if value:
                raise OperationFailure("Unrecognized field '%s'" % kwarg)
        return Cursor(self, spec, sort, projection, skip, limit,
                      collation=collation).max_time_ms(max_time_ms)

    def _get_dataset(self, spec, sort, fields, as_class):
        dataset = self._iter_documents(spec)
        if sort:
            for sort_key, sort_direction in reversed(sort):
                if sort_key == '$natural':
                    if sort_direction < 0:
                        dataset = iter(reversed(list(dataset)))
                    continue
                if sort_key.startswith('$'):
                    raise NotImplementedError(
                        'Sorting by {} is not implemented in mongomock yet'.format(sort_key))
                dataset = iter(sorted(
                    dataset, key=lambda x: filtering.resolve_sort_key(sort_key, x),
                    reverse=sort_direction < 0))
        for document in dataset:
            yield self._copy_only_fields(document, fields, as_class)

    def _copy_field(self, obj, container):
        if isinstance(obj, list):
            new = []
            for item in obj:
                new.append(self._copy_field(item, container))
            return new
        if isinstance(obj, dict):
            new = container()
            for key, value in obj.items():
                new[key] = self._copy_field(value, container)
            return new
        return copy.copy(obj)

    def _extract_projection_operators(self, fields):
        """Removes and returns fields with projection operators."""
        result = {}
        allowed_projection_operators = {'$elemMatch', '$slice'}
        for key, value in iteritems(fields):
            if isinstance(value, dict):
                for op in value:
                    if op not in allowed_projection_operators:
                        raise ValueError('Unsupported projection option: {}'.format(op))
                result[key] = value

        for key in result:
            del fields[key]

        return result

    def _apply_projection_operators(self, ops, doc, doc_copy):
        """Applies projection operators to copied document."""
        for field, op in iteritems(ops):
            if field not in doc_copy:
                if field in doc:
                    # field was not copied yet (since we are in include mode)
                    doc_copy[field] = doc[field]
                else:
                    # field doesn't exist in original document, no work to do
                    continue

            if '$slice' in op:
                if not isinstance(doc_copy[field], list):
                    raise OperationFailure(
                        'Unsupported type {} for slicing operation: {}'.format(
                            type(doc_copy[field]), op))
                op_value = op['$slice']
                slice_ = None
                if isinstance(op_value, list):
                    if len(op_value) != 2:
                        raise OperationFailure(
                            'Unsupported slice format {} for slicing operation: {}'.format(
                                op_value, op))
                    skip, limit = op_value
                    if skip < 0:
                        skip = len(doc_copy[field]) + skip
                    last = min(skip + limit, len(doc_copy[field]))
                    slice_ = slice(skip, last)
                elif isinstance(op_value, int):
                    count = op_value
                    start = 0
                    end = len(doc_copy[field])
                    if count < 0:
                        start = max(0, len(doc_copy[field]) + count)
                    else:
                        end = min(count, len(doc_copy[field]))
                    slice_ = slice(start, end)

                if slice_:
                    doc_copy[field] = doc_copy[field][slice_]
                else:
                    raise OperationFailure(
                        'Unsupported slice value {} for slicing operation: {}'.format(
                            op_value, op))

            if '$elemMatch' in op:
                if isinstance(doc_copy[field], list):
                    # find the first item that matches
                    matched = False
                    for item in doc_copy[field]:
                        if filter_applies(op['$elemMatch'], item):
                            matched = True
                            doc_copy[field] = [item]
                            break

                    # nothing have matched
                    if not matched:
                        del doc_copy[field]

                else:
                    # remove the field since there is nothing to iterate
                    del doc_copy[field]

    def _copy_only_fields(self, doc, fields, container):
        """Copy only the specified fields."""

        if fields is None:
            return self._copy_field(doc, container)

        if not fields:
            fields = {'_id': 1}
        if not isinstance(fields, dict):
            fields = helpers.fields_list_to_dict(fields)

        # we can pass in something like {'_id':0, 'field':1}, so pull the id
        # value out and hang on to it until later
        id_value = fields.pop('_id', 1)

        # filter out fields with projection operators, we will take care of them later
        projection_operators = self._extract_projection_operators(fields)

        # other than the _id field, all fields must be either includes or
        # excludes, this can evaluate to 0
        if len(set(list(fields.values()))) > 1:
            raise ValueError(
                'You cannot currently mix including and excluding fields.')

        # if we have novalues passed in, make a doc_copy based on the
        # id_value
        if not fields:
            if id_value == 1:
                doc_copy = container()
            else:
                doc_copy = self._copy_field(doc, container)
        else:
            doc_copy = _project_by_spec(
                doc, _combine_projection_spec(fields),
                is_include=list(fields.values())[0],
                container=container)

        # set the _id value if we requested it, otherwise remove it
        if id_value == 0:
            doc_copy.pop('_id', None)
        else:
            if '_id' in doc:
                doc_copy['_id'] = doc['_id']

        fields['_id'] = id_value  # put _id back in fields

        # time to apply the projection operators and put back their fields
        self._apply_projection_operators(projection_operators, doc, doc_copy)
        for field, op in iteritems(projection_operators):
            fields[field] = op
        return doc_copy

    def _update_document_fields(self, doc, fields, updater):
        """Implements the $set behavior on an existing document"""
        for k, v in iteritems(fields):
            self._update_document_single_field(doc, k, v, updater)

    def _update_document_fields_positional(self, doc, fields, spec, updater,
                                           subdocument=None):
        """Implements the $set behavior on an existing document"""
        for k, v in iteritems(fields):
            if '$' in k:

                field_name_parts = k.split('.')
                if not subdocument:
                    current_doc = doc
                    subspec = spec
                    for part in field_name_parts[:-1]:
                        if part == '$':
                            subspec = subspec.get('$elemMatch', subspec)
                            for item in current_doc:
                                if filter_applies(subspec, item):
                                    current_doc = item
                                    break
                            continue

                        new_spec = {}
                        for el in subspec:
                            if el.startswith(part):
                                if len(el.split('.')) > 1:
                                    new_spec['.'.join(
                                        el.split('.')[1:])] = subspec[el]
                                else:
                                    new_spec = subspec[el]
                        subspec = new_spec
                        current_doc = current_doc[part]

                    subdocument = current_doc
                    if field_name_parts[-1] == '$' and isinstance(subdocument, list):
                        for i, doc in enumerate(subdocument):
                            if filter_applies(subspec, doc):
                                subdocument[i] = v
                                break
                        continue

                updater(subdocument, field_name_parts[-1], v)
                continue
            # otherwise, we handle it the standard way
            self._update_document_single_field(doc, k, v, updater)

        return subdocument

    def _update_document_fields_with_positional_awareness(self, existing_document, v, spec,
                                                          updater, subdocument):
        positional = any('$' in key for key in iterkeys(v))

        if positional:
            return self._update_document_fields_positional(
                existing_document, v, spec, updater, subdocument)
        self._update_document_fields(existing_document, v, updater)
        return subdocument

    def _update_document_single_field(self, doc, field_name, field_value, updater):
        field_name_parts = field_name.split('.')
        for part in field_name_parts[:-1]:
            if isinstance(doc, list):
                try:
                    if part == '$':
                        doc = doc[0]
                    else:
                        doc = doc[int(part)]
                    continue
                except ValueError:
                    pass
            elif isinstance(doc, dict):
                if updater is _unset_updater and part not in doc:
                    # If the parent doesn't exists, so does it child.
                    return
                doc = doc.setdefault(part, {})
            else:
                return
        field_name = field_name_parts[-1]
        updater(doc, field_name, field_value)

    def _iter_documents(self, filter):
        # Validate the filter even if no documents can be returned.
        if self._store.is_empty:
            filter_applies(filter, {})

        return (document for document in list(self._store.documents)
                if filter_applies(filter, document))

    def find_one(self, filter=None, *args, **kwargs):  # pylint: disable=keyword-arg-before-vararg
        # Allow calling find_one with a non-dict argument that gets used as
        # the id for the query.
        if filter is None:
            filter = {}
        if not isinstance(filter, Mapping):
            filter = {'_id': filter}

        try:
            return next(self.find(filter, *args, **kwargs))
        except StopIteration:
            return None

    def find_one_and_delete(self, filter, projection=None, sort=None, **kwargs):
        kwargs['remove'] = True
        validate_is_mapping('filter', filter)
        return self._find_and_modify(filter, projection, sort=sort, **kwargs)

    def find_one_and_replace(self, filter, replacement,
                             projection=None, sort=None, upsert=False,
                             return_document=ReturnDocument.BEFORE, **kwargs):
        validate_is_mapping('filter', filter)
        validate_ok_for_replace(replacement)
        return self._find_and_modify(filter, projection, replacement, upsert,
                                     sort, return_document, **kwargs)

    def find_one_and_update(self, filter, update,
                            projection=None, sort=None, upsert=False,
                            return_document=ReturnDocument.BEFORE, **kwargs):
        validate_is_mapping('filter', filter)
        validate_ok_for_update(update)
        return self._find_and_modify(filter, projection, update, upsert,
                                     sort, return_document, **kwargs)

    def find_and_modify(self, query={}, update=None, upsert=False, sort=None,
                        full_response=False, manipulate=False, fields=None, **kwargs):
        warnings.warn('find_and_modify is deprecated, use find_one_and_delete'
                      ', find_one_and_replace, or find_one_and_update instead',
                      DeprecationWarning, stacklevel=2)
        if 'projection' in kwargs:
            raise TypeError("find_and_modify() got an unexpected keyword argument 'projection'")
        return self._find_and_modify(query, update=update, upsert=upsert,
                                     sort=sort, projection=fields, **kwargs)

    def _find_and_modify(self, query, projection=None, update=None,
                         upsert=False, sort=None,
                         return_document=ReturnDocument.BEFORE, session=None, **kwargs):
        if session:
            raise_not_implemented('session', 'Mongomock does not handle sessions yet')
        remove = kwargs.get('remove', False)
        if kwargs.get('new', False) and remove:
            # message from mongodb
            raise OperationFailure("remove and returnNew can't co-exist")

        if not (remove or update):
            raise ValueError('Must either update or remove')

        if remove and update:
            raise ValueError("Can't do both update and remove")

        old = self.find_one(query, projection=projection, sort=sort)
        if not old and not upsert:
            return

        if old and '_id' in old:
            query = {'_id': old['_id']}

        if remove:
            self.delete_one(query)
        else:
            updated = self._update(query, update, upsert)
            if updated['upserted']:
                query = {'_id': updated['upserted']}

        if return_document is ReturnDocument.AFTER or kwargs.get('new'):
            return self.find_one(query, projection)
        return old

    def save(self, to_save, manipulate=True, check_keys=True, **kwargs):
        warnings.warn('save is deprecated. Use insert_one or replace_one '
                      'instead', DeprecationWarning, stacklevel=2)
        validate_is_mutable_mapping('to_save', to_save)
        validate_write_concern_params(**kwargs)

        if '_id' not in to_save:
            return self.insert(to_save)
        self._update({'_id': to_save['_id']}, to_save, True, manipulate, check_keys=True, **kwargs)
        return to_save.get('_id', None)

    def delete_one(self, filter, collation=None, hint=None, session=None):
        validate_is_mapping('filter', filter)
        return DeleteResult(
            self._delete(filter, collation=collation, hint=hint, session=session), True)

    def delete_many(self, filter, collation=None, hint=None, session=None):
        validate_is_mapping('filter', filter)
        return DeleteResult(
            self._delete(filter, collation=collation, hint=hint, multi=True, session=session), True)

    def _delete(self, filter, collation=None, hint=None, multi=False, session=None):
        if hint:
            raise NotImplementedError(
                'The hint argument of delete is valid but has not been implemented in '
                'mongomock yet')
        if collation:
            raise_not_implemented(
                'collation',
                'The collation argument of delete is valid but has not been '
                'implemented in mongomock yet')
        if session:
            raise_not_implemented('session', 'Mongomock does not handle sessions yet')
        filter = helpers.patch_datetime_awareness_in_document(filter)
        if filter is None:
            filter = {}
        if not isinstance(filter, Mapping):
            filter = {'_id': filter}
        to_delete = list(self.find(filter))
        deleted_count = 0
        for doc in to_delete:
            doc_id = doc['_id']
            if isinstance(doc_id, dict):
                doc_id = helpers.hashdict(doc_id)
            del self._store[doc_id]
            deleted_count += 1
            if not multi:
                break

        return {
            'connectionId': self.database.client._id,
            'n': deleted_count,
            'ok': 1.0,
            'err': None,
        }

    def remove(self, spec_or_id=None, multi=True, **kwargs):
        warnings.warn('remove is deprecated. Use delete_one or delete_many '
                      'instead.', DeprecationWarning, stacklevel=2)
        validate_write_concern_params(**kwargs)
        return self._delete(spec_or_id, multi=multi)

    def count(self, filter=None, **kwargs):
        warnings.warn(
            'count is deprecated. Use estimated_document_count or '
            'count_documents instead. Please note that $where must be replaced '
            'by $expr, $near must be replaced by $geoWithin with $center, and '
            '$nearSphere must be replaced by $geoWithin with $centerSphere',
            DeprecationWarning, stacklevel=2)
        if kwargs.pop('session', None):
            raise_not_implemented('session', 'Mongomock does not handle sessions yet')
        if filter is None:
            return len(self._store)
        spec = helpers.patch_datetime_awareness_in_document(filter)
        return len(list(self._iter_documents(spec)))

    def count_documents(self, filter, **kwargs):
        if kwargs.pop('collation', None):
            raise_not_implemented(
                'collation',
                'The collation argument of count_documents is valid but has not been '
                'implemented in mongomock yet')
        if kwargs.pop('session', None):
            raise_not_implemented('session', 'Mongomock does not handle sessions yet')
        skip = kwargs.pop('skip', 0)
        if 'limit' in kwargs:
            limit = kwargs.pop('limit')
            if not isinstance(limit, (int, float)):
                raise OperationFailure('the limit must be specified as a number')
            if limit <= 0:
                raise OperationFailure('the limit must be positive')
            limit = math.floor(limit)
        else:
            limit = None
        unknown_kwargs = set(kwargs) - {'maxTimeMS', 'hint'}
        if unknown_kwargs:
            raise OperationFailure("unrecognized field '%s'" % unknown_kwargs.pop())

        spec = helpers.patch_datetime_awareness_in_document(filter)
        doc_num = len(list(self._iter_documents(spec)))
        count = max(doc_num - skip, 0)
        return count if limit is None else min(count, limit)

    def estimated_document_count(self, **kwargs):
        if kwargs.pop('session', None):
            raise ConfigurationError('estimated_document_count does not support sessions')
        unknown_kwargs = set(kwargs) - {'skip', 'limit', 'maxTimeMS', 'hint'}
        if unknown_kwargs:
            raise OperationFailure(
                "BSON field 'count.%s' is an unknown field." % list(unknown_kwargs)[0])
        return self.count_documents({}, **kwargs)

    def drop(self, session=None):
        if session:
            raise_not_implemented('session', 'Mongomock does not handle sessions yet')
        self.database.drop_collection(self.name)

    def ensure_index(self, key_or_list, cache_for=300, **kwargs):
        return self.create_index(key_or_list, cache_for, **kwargs)

    def create_index(self, key_or_list, cache_for=300, session=None, **kwargs):
        if session:
            raise_not_implemented('session', 'Mongomock does not handle sessions yet')
        index_list = helpers.create_index_list(key_or_list)
        is_unique = kwargs.pop('unique', False)
        is_sparse = kwargs.pop('sparse', False)

        index_name = kwargs.pop('name', helpers.gen_index_name(index_list))
        index_dict = {'key': index_list}
        if is_sparse:
            index_dict['sparse'] = True
        if is_unique:
            index_dict['unique'] = True
        if 'expireAfterSeconds' in kwargs and kwargs['expireAfterSeconds'] is not None:
            index_dict['expireAfterSeconds'] = kwargs.pop('expireAfterSeconds')

        existing_index = self._store.indexes.get(index_name)
        if existing_index and index_dict != existing_index:
            raise OperationFailure(
                'Index with name: %s already exists with different options' % index_name)

        # Check that documents already verify the uniquess of this new index.
        if is_unique:
            indexed = set()
            indexed_list = []
            for doc in self._store.documents:
                index = []
                for key, unused_order in index_list:
                    try:
                        index.append(helpers.get_value_by_dot(doc, key))
                    except KeyError:
                        if is_sparse:
                            continue
                        index.append(None)
                if is_sparse and not index:
                    continue
                index = tuple(index)
                try:
                    if index in indexed:
                        raise DuplicateKeyError('E11000 Duplicate Key Error', 11000)
                    indexed.add(index)
                except TypeError as err:
                    # index is not hashable.
                    if index in indexed_list:
                        raise_from(DuplicateKeyError('E11000 Duplicate Key Error', 11000), err)
                    indexed_list.append(index)

        self._store.create_index(index_name, index_dict)

        return index_name

    def create_indexes(self, indexes, session=None):
        for index in indexes:
            if not isinstance(index, IndexModel):
                raise TypeError(
                    '%s is not an instance of pymongo.operations.IndexModel' % index)

        return [
            self.create_index(
                index.document['key'].items(),
                session=session,
                expireAfterSeconds=index.document.get('expireAfterSeconds'),
                unique=index.document.get('unique', False),
                sparse=index.document.get('sparse', False),
                name=index.document.get('name'))
            for index in indexes
        ]

    def drop_index(self, index_or_name, session=None):
        if session:
            raise_not_implemented('session', 'Mongomock does not handle sessions yet')
        if isinstance(index_or_name, list):
            name = helpers.gen_index_name(index_or_name)
        else:
            name = index_or_name
        try:
            self._store.drop_index(name)
        except KeyError as err:
            raise_from(OperationFailure('index not found with name [%s]' % name), err)

    def drop_indexes(self, session=None):
        if session:
            raise_not_implemented('session', 'Mongomock does not handle sessions yet')
        self._store.indexes = {}

    def reindex(self, session=None):
        if session:
            raise_not_implemented('session', 'Mongomock does not handle sessions yet')

    def _list_all_indexes(self):
        if not self._store.is_created:
            return
        yield '_id_', {'key': [('_id', 1)]}
        for name, information in self._store.indexes.items():
            yield name, information

    def list_indexes(self, session=None):
        if session:
            raise_not_implemented('session', 'Mongomock does not handle sessions yet')
        for name, information in self._list_all_indexes():
            yield dict(
                information,
                key=dict(information['key']),
                name=name,
                v=2)

    def index_information(self, session=None):
        if session:
            raise_not_implemented('session', 'Mongomock does not handle sessions yet')
        return {
            name: dict(index, v=2)
            for name, index in self._list_all_indexes()
        }

    def map_reduce(self, map_func, reduce_func, out, full_response=False,
                   query=None, limit=0, session=None):
        if execjs is None:
            raise NotImplementedError(
                'PyExecJS is required in order to run Map-Reduce. '
                "Use 'pip install pyexecjs pymongo' to support Map-Reduce mock."
            )
        if session:
            raise_not_implemented('session', 'Mongomock does not handle sessions yet')
        if limit == 0:
            limit = None
        start_time = _get_perf_counter()
        out_collection = None
        reduced_rows = None
        full_dict = {
            'counts': {
                'input': 0,
                'reduce': 0,
                'emit': 0,
                'output': 0},
            'timeMillis': 0,
            'ok': 1.0,
            'result': None}
        map_ctx = execjs.compile('''
            function doMap(fnc, docList) {
                var mappedDict = {};
                function emit(key, val) {
                    if (key['$oid']) {
                        mapped_key = '$oid' + key['$oid'];
                    }
                    else {
                        mapped_key = key;
                    }
                    if(!mappedDict[mapped_key]) {
                        mappedDict[mapped_key] = [];
                    }
                    mappedDict[mapped_key].push(val);
                }
                mapper = eval('('+fnc+')');
                var mappedList = new Array();
                for(var i=0; i<docList.length; i++) {
                    var thisDoc = eval('('+docList[i]+')');
                    var mappedVal = (mapper).call(thisDoc);
                }
                return mappedDict;
            }
        ''')
        reduce_ctx = execjs.compile('''
            function doReduce(fnc, docList) {
                var reducedList = new Array();
                reducer = eval('('+fnc+')');
                for(var key in docList) {
                    var reducedVal = {'_id': key,
                            'value': reducer(key, docList[key])};
                    reducedList.push(reducedVal);
                }
                return reducedList;
            }
        ''')
        doc_list = [json.dumps(doc, default=json_util.default)
                    for doc in self.find(query)]
        mapped_rows = map_ctx.call('doMap', map_func, doc_list)
        reduced_rows = reduce_ctx.call('doReduce', reduce_func, mapped_rows)[:limit]
        for reduced_row in reduced_rows:
            if reduced_row['_id'].startswith('$oid'):
                reduced_row['_id'] = ObjectId(reduced_row['_id'][4:])
        reduced_rows = sorted(reduced_rows, key=lambda x: x['_id'])
        if full_response:
            full_dict['counts']['input'] = len(doc_list)
            for key in mapped_rows.keys():
                emit_count = len(mapped_rows[key])
                full_dict['counts']['emit'] += emit_count
                if emit_count > 1:
                    full_dict['counts']['reduce'] += 1
            full_dict['counts']['output'] = len(reduced_rows)
        if isinstance(out, (string_types, bytes)):
            out_collection = getattr(self.database, out)
            out_collection.drop()
            out_collection.insert(reduced_rows)
            ret_val = out_collection
            full_dict['result'] = out
        elif isinstance(out, SON) and out.get('replace') and out.get('db'):
            # Must be of the format SON([('replace','results'),('db','outdb')])
            out_db = getattr(self.database._client, out['db'])
            out_collection = getattr(out_db, out['replace'])
            out_collection.insert(reduced_rows)
            ret_val = out_collection
            full_dict['result'] = {'db': out['db'], 'collection': out['replace']}
        elif isinstance(out, dict) and out.get('inline'):
            ret_val = reduced_rows
            full_dict['result'] = reduced_rows
        else:
            raise TypeError("'out' must be an instance of string, dict or bson.SON")
        full_dict['timeMillis'] = int(round((_get_perf_counter() - start_time) * 1000))
        if full_response:
            ret_val = full_dict
        return ret_val

    def inline_map_reduce(self, map_func, reduce_func, full_response=False,
                          query=None, limit=0, session=None):
        return self.map_reduce(
            map_func, reduce_func, {'inline': 1}, full_response, query, limit, session=session)

    def distinct(self, key, filter=None, session=None):
        if session:
            raise_not_implemented('session', 'Mongomock does not handle sessions yet')
        return self.find(filter).distinct(key)

    def group(self, key, condition, initial, reduce, finalize=None):
        if execjs is None:
            raise NotImplementedError(
                'PyExecJS is required in order to use group. '
                "Use 'pip install pyexecjs pymongo' to support group mock."
            )
        reduce_ctx = execjs.compile('''
            function doReduce(fnc, docList) {
                reducer = eval('('+fnc+')');
                for(var i=0, l=docList.length; i<l; i++) {
                    try {
                        reducedVal = reducer(docList[i-1], docList[i]);
                    }
                    catch (err) {
                        continue;
                    }
                }
            return docList[docList.length - 1];
            }
        ''')

        ret_array = []
        doc_list_copy = []
        ret_array_copy = []
        reduced_val = {}
        doc_list = [doc for doc in self.find(condition)]
        for doc in doc_list:
            doc_copy = copy.deepcopy(doc)
            for doc_key in doc:
                if isinstance(doc[doc_key], ObjectId):
                    doc_copy[doc_key] = str(doc[doc_key])
                if doc_key not in key and doc_key not in reduce:
                    del doc_copy[doc_key]
            for initial_key in initial:
                if initial_key in doc.keys():
                    pass
                else:
                    doc_copy[initial_key] = initial[initial_key]
            doc_list_copy.append(doc_copy)
        doc_list = doc_list_copy
        for k1 in key:
            doc_list = sorted(doc_list, key=lambda x: filtering.resolve_key(k1, x))
        for k2 in key:
            if not isinstance(k2, string_types):
                raise TypeError(
                    'Keys must be a list of key names, '
                    'each an instance of %s' % string_types[0].__name__)
            for _, group in itertools.groupby(doc_list, lambda item: item[k2]):
                group_list = ([x for x in group])
                reduced_val = reduce_ctx.call('doReduce', reduce, group_list)
                ret_array.append(reduced_val)
        for doc in ret_array:
            doc_copy = copy.deepcopy(doc)
            for k in doc:
                if k not in key and k not in initial.keys():
                    del doc_copy[k]
            ret_array_copy.append(doc_copy)
        ret_array = ret_array_copy
        return ret_array

    def aggregate(self, pipeline, session=None, **unused_kwargs):
        in_collection = [doc for doc in self.find()]
        return aggregate.process_pipeline(in_collection, self.database, pipeline, session)

    def with_options(
            self, codec_options=None, read_preference=None, write_concern=None, read_concern=None):
        has_changes = False
        for key, options in iteritems(_WITH_OPTIONS_KWARGS):
            value = locals()[key]
            if value is None or value == getattr(self, '_' + key):
                continue
            has_changes = True
            for attr in options.attrs:
                if not hasattr(value, attr):
                    raise TypeError(
                        '{} must be an instance of {}'.format(key, options.typename))

        mongomock_codec_options.is_supported(codec_options)
        if codec_options != self.codec_options:
            has_changes = True

        if not has_changes:
            return self

        return Collection(
            self.database, self.name, write_concern=write_concern or self._write_concern,
            read_concern=read_concern or self._read_concern,
            read_preference=read_preference or self._read_preference,
            codec_options=codec_options or self._codec_options, _db_store=self._db_store)

    def rename(self, new_name, session=None, **kwargs):
        if session:
            raise_not_implemented('session', 'Mongomock does not handle sessions yet')
        return self.database.rename_collection(self.name, new_name, **kwargs)

    def bulk_write(self, requests, ordered=True, bypass_document_validation=False, session=None):
        if bypass_document_validation:
            raise NotImplementedError(
                'Skipping document validation is a valid MongoDB operation;'
                ' however Mongomock does not support it yet.')
        if session:
            raise_not_implemented(
                'session',
                'Sessions are valid in MongoDB 3.6 and newer; however Mongomock'
                ' does not support them yet.')
        bulk = BulkOperationBuilder(self, ordered=ordered)
        for operation in requests:
            operation._add_to_bulk(bulk)
        return BulkWriteResult(bulk.execute(), True)

    def find_raw_batches(
            self, filter=None, projection=None, skip=0, limit=0, no_cursor_timeout=False,
            cursor_type=None, sort=None, allow_partial_results=False,
            oplog_replay=False, modifiers=None, batch_size=0, manipulate=True, collation=None,
            hint=None, max_scan=None, max_time_ms=None, max=None, min=None, return_key=False,
            how_record_id=False, snapshot=False, comment=None):
        raise NotImplementedError('find_raw_batches method is not implemented in mongomock yet')

    def aggregate_raw_batches(self, pipeline, **kwargs):
        raise NotImplementedError(
            'aggregate_raw_batches method is not implemented in mongomock yet')


class Cursor(object):

    def __init__(self, collection, spec=None, sort=None, projection=None, skip=0, limit=0,
                 collation=None, no_cursor_timeout=False, batch_size=0, session=None):
        super(Cursor, self).__init__()
        self.collection = collection
        spec = helpers.patch_datetime_awareness_in_document(spec)
        self._spec = spec
        self._sort = sort
        self._projection = projection
        self._skip = skip
        self._factory_last_generated_results = None
        self._results = None
        self._factory = functools.partial(
            collection._get_dataset, spec, sort, projection, dict)
        # pymongo limit defaults to 0, returning everything
        self._limit = limit if limit != 0 else None
        self._collation = collation
        self.session = session
        self.rewind()

    def _compute_results(self, with_limit_and_skip=False):
        # Recompute the result only if the query has changed
        if not self._results or self._factory_last_generated_results != self._factory:
            if self.collection.codec_options.tz_aware:
                results = [helpers.make_datetime_timezone_aware_in_document(x)
                           for x in self._factory()]
            else:
                results = list(self._factory())
            self._factory_last_generated_results = self._factory
            self._results = results
        if with_limit_and_skip:
            results = self._results[self._skip:]
            if self._limit:
                results = results[:abs(self._limit)]
        else:
            results = self._results
        return results

    def __iter__(self):
        return self

    def clone(self):
        cursor = Cursor(self.collection,
                        self._spec, self._sort, self._projection, self._skip, self._limit)
        cursor._factory = self._factory
        return cursor

    def __next__(self):
        try:
            doc = self._compute_results(with_limit_and_skip=True)[self._emitted]
            self._emitted += 1
            return doc
        except IndexError as err:
            raise_from(StopIteration(), err)

    next = __next__

    def rewind(self):
        self._emitted = 0

    def sort(self, key_or_list, direction=None):
        sort = helpers.create_index_list(key_or_list, direction)
        if not sort:
            raise ValueError('key_or_list must not be the empty list')
        self._sort = sort
        self._factory = functools.partial(
            self.collection._get_dataset, self._spec, self._sort, self._projection, dict)
        return self

    def count(self, with_limit_and_skip=False):
        warnings.warn(
            'count is deprecated. Use Collection.count_documents instead.',
            DeprecationWarning, stacklevel=2)
        results = self._compute_results(with_limit_and_skip)
        return len(results)

    def skip(self, count):
        self._skip = count
        return self

    def limit(self, count):
        self._limit = count if count != 0 else None
        return self

    def batch_size(self, count):
        return self

    def close(self):
        pass

    def hint(self, unused_hint):
        if self._emitted:
            raise InvalidOperation('cannot set options after executing query')
        # TODO(pascal): Once we implement $text indexes and queries, raise an
        # exception if hint is used on a $text query.
        # https://docs.mongodb.com/manual/reference/method/cursor.hint/#behavior
        return self

    def distinct(self, key, session=None):
        if session:
            raise_not_implemented('session', 'Mongomock does not handle sessions yet')
        if not isinstance(key, string_types):
            raise TypeError('cursor.distinct key must be a string')
        unique = set()
        unique_dict_vals = []
        for x in self._compute_results():
            for value in filtering.iter_key_candidates(key, x):
                if value == NOTHING:
                    continue
                if isinstance(value, dict):
                    if any(dict_val == value for dict_val in unique_dict_vals):
                        continue
                    unique_dict_vals.append(value)
                else:
                    unique.update(
                        value if isinstance(
                            value, (tuple, list)) else [value])
        return list(unique) + unique_dict_vals

    def __getitem__(self, index):
        if isinstance(index, slice):
            if index.step is not None:
                raise IndexError('Cursor instances do not support slice steps')

            skip = 0
            if index.start is not None:
                if index.start < 0:
                    raise IndexError('Cursor instances do not support'
                                     'negative indices')
                skip = index.start

            if index.stop is not None:
                limit = index.stop - skip
                if limit < 0:
                    raise IndexError('stop index must be greater than start'
                                     'index for slice %r' % index)
                if limit == 0:
                    self.__empty = True
            else:
                limit = 0

            self._skip = skip
            self._limit = limit
            return self
        if not isinstance(index, int):
            raise TypeError("index '%s' cannot be applied to Cursor instances" % index)
        if index < 0:
            raise IndexError('Cursor instances do not support negativeindices')
        return self._compute_results(with_limit_and_skip=True)[index]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @property
    def alive(self):
        return self._emitted != self.count()

    @property
    def collation(self):
        return self._collation

    def max_time_ms(self, max_time_ms):
        if max_time_ms is not None and not isinstance(max_time_ms, int):
            raise TypeError('max_time_ms must be an integer or None')
        # Currently the value is ignored as mongomock never times out.
        return self


def _set_updater(doc, field_name, value):
    if isinstance(value, (tuple, list)):
        value = copy.deepcopy(value)
    if BSON:
        # bson validation
        BSON.encode({field_name: value}, check_keys=True)
    if isinstance(doc, dict):
        doc[field_name] = value
    if isinstance(doc, list):
        field_index = int(field_name)
        if field_index < 0:
            raise WriteError('Negative index provided')
        len_diff = field_index - (len(doc) - 1)
        if len_diff > 0:
            doc += [None] * len_diff
        doc[field_index] = value


def _unset_updater(doc, field_name, value):
    if isinstance(doc, dict):
        doc.pop(field_name, None)


def _inc_updater(doc, field_name, value):
    if isinstance(doc, dict):
        doc[field_name] = doc.get(field_name, 0) + value

    if isinstance(doc, list):
        field_index = int(field_name)
        if field_index < 0:
            raise WriteError('Negative index provided')
        try:
            doc[field_index] += value
        except IndexError:
            len_diff = field_index - (len(doc) - 1)
            doc += [None] * len_diff
            doc[field_index] = value


def _max_updater(doc, field_name, value):
    if isinstance(doc, dict):
        doc[field_name] = max(doc.get(field_name, value), value)


def _min_updater(doc, field_name, value):
    if isinstance(doc, dict):
        doc[field_name] = min(doc.get(field_name, value), value)


def _pop_updater(doc, field_name, value):
    if value not in {1, -1}:
        raise WriteError('$pop expects 1 or -1, found: ' + str(value))

    if isinstance(doc, dict):
        if isinstance(doc[field_name], (tuple, list)):
            doc[field_name] = list(doc[field_name])
            _pop_from_list(doc[field_name], value)
            return
        raise WriteError('Path contains element of non-array type')

    if isinstance(doc, list):
        field_index = int(field_name)
        if field_index < 0:
            raise WriteError('Negative index provided')
        if field_index >= len(doc):
            return
        _pop_from_list(doc[field_index], value)


def _pop_from_list(list_instance, mongo_pop_value):
    if not list_instance:
        return

    if mongo_pop_value == 1:
        list_instance.pop()
    elif mongo_pop_value == -1:
        list_instance.pop(0)


def _current_date_updater(doc, field_name, value):
    if isinstance(doc, dict):
        if value == {'$type': 'timestamp'}:
            # TODO(juannyg): get_current_timestamp should also be using helpers utcnow,
            # as it currently using time.time internally
            doc[field_name] = helpers.get_current_timestamp()
        else:
            doc[field_name] = mongomock.utcnow()


_updaters = {
    '$set': _set_updater,
    '$unset': _unset_updater,
    '$inc': _inc_updater,
    '$max': _max_updater,
    '$min': _min_updater,
    '$pop': _pop_updater
}
