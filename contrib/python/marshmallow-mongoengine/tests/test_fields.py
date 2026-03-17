# -*- coding: utf-8 -*-
from __future__ import absolute_import
import datetime as dt
import decimal
from datetime import datetime

import mongoengine as me

from marshmallow import validate, Schema
from marshmallow.exceptions import ValidationError

import pytest
from . import exception_test
from marshmallow_mongoengine import (fields, fields_for_model, ModelSchema,
                                     ModelConverter, convert_field, field_for)


def contains_validator(field, v_type):
    for v in field.validators:
        if isinstance(v, v_type):
            return v
    return False



class TestFields(object):

    @exception_test
    def test_FileField(self, mongoengine_connection):
        class File(me.Document):
            name = me.StringField(primary_key=True)
            file = me.FileField()

        class FileSchema(ModelSchema):
            class Meta:
                model = File
        doc = File(name='test_file')
        data = b'1234567890' * 10
        doc.file.put(data, content_type='application/octet-stream')
        dump = FileSchema().dump(doc)
        assert dump == {'name': 'test_file'}
        # Should not be able to load the file
        load = FileSchema().load({'name': 'bad_load', 'file': b'12345'})
        assert not load.file

    @exception_test
    def test_ListField(self, mongoengine_connection):
        class Doc(me.Document):
            list = me.ListField(me.StringField())
        fields_ = fields_for_model(Doc)
        assert type(fields_['list']) is fields.List

        class DocSchema(ModelSchema):
            class Meta:
                model = Doc
        list_ = ['A', 'B', 'C']
        doc = Doc(list=list_)
        dump_data = DocSchema().dump(doc)
        assert dump_data == {'list': list_}
        load_data = DocSchema().load(dump_data)
        assert load_data.list == list_

    @exception_test
    def test_ListSpecialField(self, mongoengine_connection):
        class NestedDoc(me.EmbeddedDocument):
            field = me.StringField()

        class Doc(me.Document):
            list = me.ListField(me.EmbeddedDocumentField(NestedDoc))
        fields_ = fields_for_model(Doc)
        assert type(fields_['list']) is fields.List
        assert type(fields_['list'].inner) is fields.Nested
        class DocSchema(ModelSchema):
            class Meta:
                model = Doc
        list_ = [{'field': 'A'}, {'field': 'B'}, {'field': 'C'}]
        doc = Doc(list=list_)
        dump_data = DocSchema().dump(doc)
        assert dump_data == {'list': list_}
        load_data = DocSchema().load(dump_data)
        for i, elem in enumerate(list_):
            assert load_data.list[i].field == elem['field']

    @exception_test
    def test_DictField(self, mongoengine_connection):
        class Doc(me.Document):
            data = me.DictField()
        fields_ = fields_for_model(Doc)
        assert type(fields_['data']) is fields.Raw

        class DocSchema(ModelSchema):
            class Meta:
                model = Doc
        data = {
            'int_1': 1,
            'nested_2': {
                'sub_int_1': 42,
                'sub_list_2': []
            },
            'list_3': ['a', 'b', 'c']
        }
        doc = Doc(data=data)
        dump_data = DocSchema().dump(doc)
        assert dump_data == {'data': data}
        load_doc = DocSchema().load(dump_data)
        assert load_doc._data == doc._data

    def test_DynamicField(self, mongoengine_connection):
        class Doc(me.Document):
            dynamic = me.DynamicField()
        fields_ = fields_for_model(Doc)
        assert type(fields_['dynamic']) is fields.Raw

        class DocSchema(ModelSchema):
            class Meta:
                model = Doc
        data = {
            'int_1': 1,
            'nested_2': {
                'sub_int_1': 42,
                'sub_list_2': []
            },
            'list_3': ['a', 'b', 'c']
        }
        doc = Doc(dynamic=data)
        dump_data = DocSchema().dump(doc)
        assert dump_data == {'dynamic': data}
        load_data = DocSchema().load(dump_data)
        assert load_data.dynamic == data

    def test_GenericReferenceField(self, mongoengine_connection):
        class Doc(me.Document):
            id = me.StringField(primary_key=True, default='main')
            generic = me.GenericReferenceField()

        class SubDocA(me.Document):
            field_a = me.StringField(primary_key=True, default='doc_a_pk')

        class SubDocB(me.Document):
            field_b = me.IntField(primary_key=True, default=42)
        fields_ = fields_for_model(Doc)
        assert type(fields_['generic']) is fields.GenericReference

        class DocSchema(ModelSchema):
            class Meta:
                model = Doc
        # Test dump
        sub_doc_a = SubDocA().save()
        sub_doc_b = SubDocB().save()
        doc = Doc(generic=sub_doc_a)
        dump_data = DocSchema().dump(doc)
        assert dump_data == {'generic': 'doc_a_pk', 'id': 'main'}
        doc.generic = sub_doc_b
        doc.save()
        dump_data = DocSchema().dump(doc)
        assert dump_data == {'generic': 42, 'id': 'main'}
        # Test load
        for bad_generic in (
                {'id': str(sub_doc_a.id)}, {'_cls': sub_doc_a._class_name},
                {'id': str(sub_doc_a.id), '_cls': sub_doc_b._class_name},
                {'id': 'not_an_id', '_cls': sub_doc_a._class_name},
                {'id': 42, '_cls': sub_doc_a._class_name},
                {'id': 'main', '_cls': sub_doc_b._class_name},
                {'id': str(sub_doc_a.id), '_cls': 'not_a_class'},
                {'id': None, '_cls': sub_doc_a._class_name},
                {'id': str(sub_doc_a.id), '_cls': None},
            ):
            with pytest.raises(ValidationError) as excinfo:
                load = DocSchema().load({"generic": bad_generic})
            assert 'generic' in excinfo.value.args[0]
        load_data = DocSchema().load({"generic": {"id": str(sub_doc_a.id),
                                             "_cls": sub_doc_a._class_name}})
        assert load_data['generic'] == sub_doc_a
        load_data = DocSchema().load({"generic": {"id": str(sub_doc_b.id),
                                             "_cls": sub_doc_b._class_name}})
        assert load_data['generic'] == sub_doc_b
        # Teste choices param

        class DocOnlyA(me.Document):
            id = me.StringField(primary_key=True, default='main')
            generic = me.GenericReferenceField(choices=[SubDocA])

        class DocOnlyASchema(ModelSchema):
            class Meta:
                model = DocOnlyA
        load_data = DocOnlyASchema().load({})
        load_data = DocOnlyASchema().load({"generic": {"id": str(sub_doc_a.id),
                                                  "_cls": sub_doc_a._class_name}})
        assert load_data['generic'] == sub_doc_a
        with pytest.raises(ValidationError) as excinfo:
            load = DocOnlyASchema().load({"generic": {"id": str(sub_doc_b.id),
                                                  "_cls": sub_doc_b._class_name}})
        assert 'generic' in excinfo.value.args[0]

    @pytest.mark.skipif(
        not hasattr(me, 'GenericLazyReferenceField'),
        reason='GenericLazyReferenceField requires mongoengine>=0.15.0')
    def test_GenericLazyReferenceField(self, mongoengine_connection):
        class Doc(me.Document):
            id = me.StringField(primary_key=True, default='main')
            generic = me.GenericLazyReferenceField()
        class SubDocA(me.Document):
            field_a = me.StringField(primary_key=True, default='doc_a_pk')
        class SubDocB(me.Document):
            field_b = me.IntField(primary_key=True, default=42)
        fields_ = fields_for_model(Doc)
        assert type(fields_['generic']) is fields.GenericReference
        class DocSchema(ModelSchema):
            class Meta:
                model = Doc
        # Test dump
        sub_doc_a = SubDocA().save()
        sub_doc_b = SubDocB().save()
        doc = Doc(generic=sub_doc_a)
        dump_data = DocSchema().dump(doc)
        assert dump_data == {'generic': 'doc_a_pk', 'id': 'main'}
        doc.generic = sub_doc_b
        doc.save()
        dump_data = DocSchema().dump(doc)
        assert dump_data == {'generic': 42, 'id': 'main'}
        # Test load
        for bad_generic in (
                {'id': str(sub_doc_a.id)}, {'_cls': sub_doc_a._class_name},
                {'id': str(sub_doc_a.id), '_cls': sub_doc_b._class_name},
                {'id': 'not_an_id', '_cls': sub_doc_a._class_name},
                {'id': 42, '_cls': sub_doc_a._class_name},
                {'id': 'main', '_cls': sub_doc_b._class_name},
                {'id': str(sub_doc_a.id), '_cls': 'not_a_class'},
                {'id': None, '_cls': sub_doc_a._class_name},
                {'id': str(sub_doc_a.id), '_cls': None},
            ):
            with pytest.raises(ValidationError) as excinfo:
                load = DocSchema().load({"generic": bad_generic})
            assert 'generic' in excinfo.value.args[0]
        load_data = DocSchema().load({"generic": {"id": str(sub_doc_a.id),
                                             "_cls": sub_doc_a._class_name}})
        assert load_data['generic'] == sub_doc_a
        load_data = DocSchema().load({"generic": {"id": str(sub_doc_b.id),
                                             "_cls": sub_doc_b._class_name}})
        assert load_data['generic'] == sub_doc_b
        # Teste choices param
        class DocOnlyA(me.Document):
            id = me.StringField(primary_key=True, default='main')
            generic = me.GenericLazyReferenceField(choices=[SubDocA])
        class DocOnlyASchema(ModelSchema):
            class Meta:
                model = DocOnlyA
        load_data = DocOnlyASchema().load({})
        load_data = DocOnlyASchema().load({"generic": {"id": str(sub_doc_a.id),
                                                  "_cls": sub_doc_a._class_name}})
        assert load_data['generic'] == sub_doc_a

    @exception_test
    def test_GenericEmbeddedDocumentField(self, mongoengine_connection):
        class Doc(me.Document):
            id = me.StringField(primary_key=True, default='main')
            embedded = me.GenericEmbeddedDocumentField()

        class EmbeddedA(me.EmbeddedDocument):
            field_a = me.StringField(default='field_a_value')

        class EmbeddedB(me.EmbeddedDocument):
            field_b = me.IntField(default=42)
        fields_ = fields_for_model(Doc)
        assert type(fields_['embedded']) is fields.GenericEmbeddedDocument

        class DocSchema(ModelSchema):
            class Meta:
                model = Doc
        doc = Doc(embedded=EmbeddedA())
        dump_data = DocSchema().dump(doc)
        assert dump_data == {'embedded': {'field_a': 'field_a_value'}, 'id': 'main'}
        doc.embedded = EmbeddedB()
        doc.save()
        dump_data = DocSchema().dump(doc)
        assert dump_data == {'embedded': {'field_b': 42}, 'id': 'main'}
        # TODO: test load ?

    @exception_test
    def test_MapField(self, mongoengine_connection):
        class MappedDoc(me.EmbeddedDocument):
            field = me.StringField()

        class Doc(me.Document):
            id = me.IntField(primary_key=True, default=1)
            map = me.MapField(me.EmbeddedDocumentField(MappedDoc))
            str = me.MapField(me.StringField())
        fields_ = fields_for_model(Doc)
        assert type(fields_['map']) is fields.Map

        class DocSchema(ModelSchema):
            class Meta:
                model = Doc
        doc = Doc(map={'a': MappedDoc(field='A'), 'b': MappedDoc(field='B')},
                  str={'a': 'aaa', 'b': 'bbbb'}).save()
        dump_data = DocSchema().dump(doc)
        assert dump_data == {'map': {'a': {'field': 'A'}, 'b': {'field': 'B'}},
                             'str': {'a': 'aaa', 'b': 'bbbb'}, 'id': 1}
        # Try the load
        load_data = DocSchema().load(dump_data)
        assert load_data.map == doc.map

    @exception_test
    def test_ReferenceField(self, mongoengine_connection):
        class ReferenceDoc(me.Document):
            field = me.IntField(primary_key=True, default=42)

        class Doc(me.Document):
            id = me.StringField(primary_key=True, default='main')
            ref = me.ReferenceField(ReferenceDoc)
        fields_ = fields_for_model(Doc)
        assert type(fields_['ref']) is fields.Reference

        class DocSchema(ModelSchema):
            class Meta:
                model = Doc
        ref_doc = ReferenceDoc().save()
        doc = Doc(ref=ref_doc)
        dump_data = DocSchema().dump(doc)
        assert dump_data == {'ref': 42, 'id': 'main'}
        # Try the same with reference document type passed as string

        class DocSchemaRefAsString(Schema):
            id = fields.String()
            ref = fields.Reference('ReferenceDoc')
        dump_data = DocSchemaRefAsString().dump(doc)
        assert dump_data == {'ref': 42, 'id': 'main'}
        # Test the field loading
        load_data = DocSchemaRefAsString().load(dump_data)
        assert type(load_data['ref']) == ReferenceDoc
        # Try invalid loads
        for bad_ref, error_msg in (
                (1, ['unknown document ReferenceDoc `1`']),
                ('NaN', ['unknown document ReferenceDoc `NaN`']),
                (None, ['Field may not be null.']),
        ):
            dump_data['ref'] = bad_ref
            with pytest.raises(ValidationError) as excinfo:
                _ = DocSchemaRefAsString().load(dump_data)
            assert excinfo.value.args[0] == {'ref': error_msg}

    @pytest.mark.skipif(
        not hasattr(me, 'LazyReferenceField'),
        reason='LazyReferenceField requires mongoengine>=0.15.0')
    def test_LazyReferenceField(self, mongoengine_connection):
        class ReferenceDoc(me.Document):
            field = me.IntField(primary_key=True, default=42)
        class Doc(me.Document):
            id = me.StringField(primary_key=True, default='main')
            ref = me.LazyReferenceField(ReferenceDoc)
        fields_ = fields_for_model(Doc)
        assert type(fields_['ref']) is fields.Reference
        class DocSchema(ModelSchema):
            class Meta:
                model = Doc
        ref_doc = ReferenceDoc().save()
        doc = Doc(ref=ref_doc)
        dump_data = DocSchema().dump(doc)
        assert dump_data == {'ref': 42, 'id': 'main'}
        # Force ref field to be LazyReference
        doc.save()
        doc.reload()
        dump_data = DocSchema().dump(doc)
        assert dump_data == {'ref': 42, 'id': 'main'}
        # Try the same with reference document type passed as string
        class DocSchemaRefAsString(Schema):
            id = fields.String()
            ref = fields.Reference('ReferenceDoc')
        dump_data = DocSchemaRefAsString().dump(doc)
        assert dump_data == {'ref': 42, 'id': 'main'}
        # Test the field loading
        load_data = DocSchemaRefAsString().load(dump_data)
        assert type(load_data['ref']) == ReferenceDoc
        # Try invalid loads
        for bad_ref, error_msg in (
                (1, ['unknown document ReferenceDoc `1`']),
                ('NaN', ['unknown document ReferenceDoc `NaN`']),
                (None, ['Field may not be null.']),
        ):
            dump_data['ref'] = bad_ref
            with pytest.raises(ValidationError) as excinfo:
                _ = DocSchemaRefAsString().load(dump_data)
            assert excinfo.value.args[0] == {'ref': error_msg}

    @exception_test
    def test_PointField(self, mongoengine_connection):
        class Doc(me.Document):
            point = me.PointField()

        class DocSchema(ModelSchema):
            class Meta:
                model = Doc
        doc = Doc(point={ 'type': 'Point', 'coordinates': [10, 20] })
        dump_data = DocSchema().dump(doc)
        assert dump_data['point'] == { 'x': 10, 'y': 20 }
        load_data = DocSchema().load(dump_data)
        assert load_data.point == { 'type': 'Point', 'coordinates': [10, 20] }
        # Deserialize Point with coordinates passed as string
        data = {'point': { 'x': '10', 'y': '20' }}
        load_data = DocSchema().load(data)
        assert load_data.point == { 'type': 'Point', 'coordinates': [10, 20] }
        # Try to load invalid coordinates
        data = {'point': { 'x': '10', 'y': '20foo' }}

        with pytest.raises(ValidationError) as excinfo:
            load_data = DocSchema().load(data)
        assert 'point' in excinfo.value.args[0]

    @exception_test
    def test_LineStringField(self, mongoengine_connection):
        class Doc(me.Document):
            line = me.LineStringField()
        class DocSchema(ModelSchema):
            class Meta:
                model = Doc
        doc = Doc(line={'type': 'LineString', 'coordinates': [[10, 20], [30, 40]]})
        dump = DocSchema().dump(doc)
        assert dump['line']['coordinates'] == [[10, 20], [30, 40]]
        load = DocSchema().load(dump)
        assert load.line == {'type': 'LineString', 'coordinates': [[10, 20], [30, 40]]}
        # Deserialize LineString with coordinates passed as strings
        data = {'line': {'coordinates': [['10', '20'], ['30', '40']]}}
        load = DocSchema().load(data)
        assert load.line == {'type': 'LineString', 'coordinates': [[10, 20], [30, 40]]}
        # Try to load invalid coordinates
        data = {'line': {'coordinates': [['10', '20foo']]}}
        with pytest.raises(ValidationError) as excinfo:
            load = DocSchema().load(data)
        assert 'line' in excinfo.value.args[0]
