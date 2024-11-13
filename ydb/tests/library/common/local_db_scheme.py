#!/usr/bin/env python
# -*- coding: utf-8 -*-
import ydb.core.protos.msgbus_pb2 as msgbus
from ydb.tests.library.common.types import DeltaTypes, PType


def column_to_dict(delta):
    return {
        "ColumnId": delta.ColumnId,
        "ColumnName": delta.ColumnName,
        "ColumnType": str(
            PType.from_int(delta.ColumnType)
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
        cid = delta.ColumnId
        fid = delta.FamilyId

        if fid not in self.data['ColumnFamilies']:
            raise RuntimeError("Unknown family")

        self.data['ColumnFamilies'][fid]['Columns'].append(cid)
        return self

    def add_family(self, delta):
        fid = delta.FamilyId
        rid = delta.RoomId

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
        fid = delta.FamilyId

        if fid not in self.data['ColumnFamilies']:
            raise RuntimeError("Unknown family")

        if hasattr(delta, 'Codec') and delta.Codec is not None:
            self.data['ColumnFamilies'][fid]['Codec'] = delta.Codec

        if hasattr(delta, 'InMemory') and delta.InMemory is not None:
            self.data['ColumnFamilies'][fid]['InMemory'] = delta.InMemory

        if hasattr(delta, 'Cache') and delta.Cache is not None:
            self.data['ColumnFamilies'][fid]['Cache'] = delta.Cache

        if hasattr(delta, 'Small') and delta.Small is not None:
            self.data['ColumnFamilies'][fid]['Small'] = delta.Small

        if hasattr(delta, 'Large') and delta.Large is not None:
            self.data['ColumnFamilies'][fid]['Large'] = delta.Large

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
        cid = delta.ColumnId
        self.data['TableKey'].append(cid)
        return self

    def unknown_delta(self, delta):
        raise RuntimeError("Unknown delta type")

    def set_room(self, delta):
        self.data['Rooms'][delta.RoomId] = {"Main": delta.Main, "Outer": delta.Outer, "Blobs": delta.Blobs, "ExternalBlobs": delta.ExternalBlobs}
        return self

    def set_compaction_policy(self, delta):
        # Not Implemented yet
        return self

    def set_table(self, delta):
        # Not Implemented yet
        return self

    def add_delta(self, delta):
        mapping = {
            DeltaTypes.AddColumn: self.add_column,
            DeltaTypes.DropColumn: self.drop_column,
            DeltaTypes.AddColumnToKey: self.add_column_to_key,
            DeltaTypes.AddColumnToFamily: self.add_column_to_family,
            DeltaTypes.AddFamily: self.add_family,
            DeltaTypes.SetRoom: self.set_room,
            DeltaTypes.SetCompactionPolicy: self.set_compaction_policy,
            DeltaTypes.SetFamily: self.set_family,
            DeltaTypes.SetTable: self.set_table,
        }
        op = mapping.get(delta.DeltaType, self.unknown_delta)
        return op(delta)


def get_deltas(client, tablet_id):
    resp = client.send_request(msgbus.TLocalSchemeTx(TabletID=tablet_id, Timeout=60 * 1000), method='LocalSchemeTx')
    return resp.LocalDbScheme.Delta


def get_scheme(client, tablet_id):
    deltas = get_deltas(client, tablet_id)
    scheme = []
    pos = {}

    for delta in deltas:
        if not delta.HasField('TableId'):
            continue

        if delta.DeltaType == DeltaTypes.AddTable:
            table = TableScheme(delta.TableId, delta.TableName)
            pos[delta.TableId] = len(scheme)
            scheme.append(table)
            continue

        scheme[pos[delta.TableId]] = scheme[pos[delta.TableId]].add_delta(delta)

    return [element for element in scheme]
