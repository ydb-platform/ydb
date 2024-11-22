#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds
from ydb.public.api.protos.draft.ydb_tablet_pb2 import ChangeTabletSchemaRequest
from ydb.tests.library.common.types import PType


def column_to_dict(delta):
    return {
        "ColumnId": delta['column_id'],
        "ColumnName": delta['column_name'],
        "ColumnType": str(
            PType.from_int(delta['column_type'])
        )
    }


class TableScheme(object):
    def __init__(self, table_id, table_name):
        self.data = {
            'TableId': table_id,
            'TableName': table_name,
            'TableKey': [],
            'ColumnsAdded': [],
            'ColumnsDropped': [],
            'ColumnFamilies': {},
            'Rooms': {}
        }

    def add_column(self, delta):
        self.data['ColumnsAdded'].append(column_to_dict(delta))
        return self

    def drop_column(self, delta):
        self.data['ColumnsDropped'].append(column_to_dict(delta))
        return self

    def add_column_to_family(self, delta):
        cid = delta['column_id']
        fid = delta['family_id']

        if fid not in self.data['ColumnFamilies']:
            raise RuntimeError("Unknown family")

        self.data['ColumnFamilies'][fid]['Columns'].append(cid)
        return self

    def add_family(self, delta):
        fid = delta['family_id']
        rid = delta['room_id']

        if rid not in self.data['Rooms']:
            raise RuntimeError("Unknown room")

        if fid in self.data['ColumnFamilies']:
            raise RuntimeError("Rewriting family is unsupported")

        self.data['ColumnFamilies'][fid] = {
            'Columns': [],
            'RoomID': rid,
            'Codec': None,
            'InMemory': None,
            'Cache': None,
            'Small': None,
            'Large': None
        }

        return self

    def set_family(self, delta):
        fid = delta['family_id']

        if fid not in self.data['ColumnFamilies']:
            raise RuntimeError("Unknown family")

        if 'codec' in delta and delta['codec'] is not None:
            self.data['ColumnFamilies'][fid]['Codec'] = delta['codec']

        if 'in_memory' in delta and delta['in_memory'] is not None:
            self.data['ColumnFamilies'][fid]['InMemory'] = delta['in_memory']

        if 'cache' in delta and delta['cache'] is not None:
            self.data['ColumnFamilies'][fid]['Cache'] = delta['cache']

        if 'small' in delta and delta['small'] is not None:
            self.data['ColumnFamilies'][fid]['Small'] = delta['small']

        if 'large' in delta and delta['large'] is not None:
            self.data['ColumnFamilies'][fid]['Large'] = delta['large']

        return self

    @property
    def ColumnFamilies(self):
        return self.data['ColumnFamilies']

    @property
    def TableName(self):
        return self.data['TableName']

    @property
    def Rooms(self):
        return self.data['Rooms']

    def add_column_to_key(self, delta):
        cid = delta['column_id']
        self.data['TableKey'].append(cid)
        return self

    def unknown_delta(self, delta):
        raise RuntimeError("Unknown delta type")

    def set_room(self, delta):
        self.data['Rooms'][delta['room_id']] = {"Main": delta['main'], "Outer": delta['outer'], "Blobs": delta['blobs'], "ExternalBlobs": delta['external_blobs']}
        return self

    def set_compaction_policy(self, delta):
        # Not Implemented yet
        return self

    def set_table(self, delta):
        # Not Implemented yet
        return self

    def add_delta(self, delta):
        mapping = {
            'AddColumn': self.add_column,
            'DropColumn': self.drop_column,
            'AddColumnToKey': self.add_column_to_key,
            'AddColumnToFamily': self.add_column_to_family,
            'AddFamily': self.add_family,
            'SetRoom': self.set_room,
            'SetCompactionPolicy': self.set_compaction_policy,
            'SetFamily': self.set_family,
            'SetTable': self.set_table,
        }
        op = mapping.get(delta['delta_type'], self.unknown_delta)
        return op(delta)


def get_deltas(client, tablet_id):
    resp = client.tablet_service.ChangeTabletSchema(ChangeTabletSchemaRequest(tablet_id=tablet_id))
    assert resp.status == StatusIds.SUCCESS
    if resp.status != StatusIds.SUCCESS:
        raise RuntimeError('ERROR: {status} {issues}'.format(status=resp.status, issues=resp.issues))
    schema = json.loads(resp.schema)
    return schema['delta']


def get_scheme(client, tablet_id):
    deltas = get_deltas(client, tablet_id)
    scheme = []
    pos = {}

    for delta in deltas:
        if 'table_id' not in delta:
            continue

        if delta['delta_type'] == 'AddTable':
            table = TableScheme(delta['table_id'], delta['table_name'])
            pos[delta['table_id']] = len(scheme)
            scheme.append(table)
            continue

        scheme[pos[delta['table_id']]] = scheme[pos[delta['table_id']]].add_delta(delta)

    return [element for element in scheme]
