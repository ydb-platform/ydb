#!/usr/bin/env python
# -*- coding: utf-8 -*-
import capabilities
from datetime import timedelta
from contextlib import closing
import unittest
import MySQLdb
from MySQLdb.compat import unicode
from MySQLdb import cursors
from configdb import connection_factory
import warnings


warnings.filterwarnings('ignore')


class test_MySQLdb(capabilities.DatabaseTest):

    db_module = MySQLdb
    connect_args = ()
    connect_kwargs = dict(use_unicode=True, sql_mode="ANSI,STRICT_TRANS_TABLES,TRADITIONAL")
    create_table_extra = "ENGINE=INNODB CHARACTER SET UTF8"
    leak_test = False

    def quote_identifier(self, ident):
        return "`%s`" % ident

    def test_TIME(self):
        def generator(row,col):
            return timedelta(0, row*8000)
        self.check_data_integrity(
                 ('col1 TIME',),
                 generator)

    def test_TINYINT(self):
        # Number data
        def generator(row, col):
            v = (row*row) % 256
            if v > 127:
                v = v-256
            return v
        self.check_data_integrity(
            ('col1 TINYINT',),
            generator)

    def test_stored_procedures(self):
        db = self.connection
        c = self.cursor
        self.create_table(('pos INT', 'tree CHAR(20)'))
        c.executemany("INSERT INTO %s (pos,tree) VALUES (%%s,%%s)" % self.table,
                      list(enumerate('ash birch cedar Lärche pine'.split())))
        db.commit()

        c.execute("""
        CREATE PROCEDURE test_sp(IN t VARCHAR(255))
        BEGIN
            SELECT pos FROM %s WHERE tree = t;
        END
        """ % self.table)
        db.commit()

        c.callproc('test_sp', ('Lärche',))
        rows = c.fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][0], 3)
        c.nextset()

        c.execute("DROP PROCEDURE test_sp")
        c.execute('drop table %s' % (self.table))

    def test_small_CHAR(self):
        # Character data
        def generator(row,col):
            i = (row*col+62)%256
            if i == 62: return ''
            if i == 63: return None
            return chr(i)
        self.check_data_integrity(
            ('col1 char(1)','col2 char(1)'),
            generator)

    def test_BIT(self):
        c = self.cursor
        try:
            c.execute("""create table test_BIT (
                b3 BIT(3),
                b7 BIT(10),
                b64 BIT(64))""")

            one64 = '1'*64
            c.execute(
                "insert into test_BIT (b3, b7, b64)"
                " VALUES (b'011', b'1111111111', b'%s')"
                % one64)

            c.execute("SELECT b3, b7, b64 FROM test_BIT")
            row = c.fetchone()
            self.assertEqual(row[0], b'\x03')
            self.assertEqual(row[1], b'\x03\xff')
            self.assertEqual(row[2], b'\xff'*8)
        finally:
            c.execute("drop table if exists test_BIT")

    def test_MULTIPOLYGON(self):
        c = self.cursor
        try:
            c.execute("""create table test_MULTIPOLYGON (
                id INTEGER PRIMARY KEY,
                border MULTIPOLYGON)""")

            c.execute(
                "insert into test_MULTIPOLYGON (id, border)"
                " VALUES (1, GeomFromText('MULTIPOLYGON(((1 1, 1 -1, -1 -1, -1 1, 1 1)),((1 1, 3 1, 3 3, 1 3, 1 1)))'))"
            )

            c.execute("SELECT id, AsText(border) FROM test_MULTIPOLYGON")
            row = c.fetchone()
            self.assertEqual(row[0], 1)
            self.assertEqual(row[1], 'MULTIPOLYGON(((1 1,1 -1,-1 -1,-1 1,1 1)),((1 1,3 1,3 3,1 3,1 1)))')

            c.execute("SELECT id, AsWKB(border) FROM test_MULTIPOLYGON")
            row = c.fetchone()
            self.assertEqual(row[0], 1)
            self.assertNotEqual(len(row[1]), 0)

            c.execute("SELECT id, border FROM test_MULTIPOLYGON")
            row = c.fetchone()
            self.assertEqual(row[0], 1)
            self.assertNotEqual(len(row[1]), 0)
        finally:
            c.execute("drop table if exists test_MULTIPOLYGON")

    def test_bug_2671682(self):
        from MySQLdb.constants import ER
        try:
            self.cursor.execute("describe some_non_existent_table");
        except self.connection.ProgrammingError as msg:
            self.assertTrue(str(ER.NO_SUCH_TABLE) in str(msg))

    def test_bug_3514287(self):
        c = self.cursor
        try:
            c.execute("""create table bug_3541287 (
                c1 CHAR(10),
                t1 TIMESTAMP)""")
            c.execute("insert into bug_3541287 (c1,t1) values (%s, NOW())",
                ("blah",))
        finally:
            c.execute("drop table if exists bug_3541287")

    def test_ping(self):
        self.connection.ping()

    def test_reraise_exception(self):
        c = self.cursor
        try:
            c.execute("SELECT x FROM not_existing_table")
        except MySQLdb.ProgrammingError as e:
            self.assertEqual(e.args[0], 1146)
            return
        self.fail("Should raise ProgrammingError")

    def test_binary_prefix(self):
        # verify prefix behaviour when enabled, disabled and for default (disabled)
        for binary_prefix in (True, False, None):
            kwargs = self.connect_kwargs.copy()
            # needs to be set to can guarantee CHARSET response for normal strings
            kwargs['charset'] = 'utf8'
            if binary_prefix != None:
                kwargs['binary_prefix'] = binary_prefix

            with closing(connection_factory(**kwargs)) as conn:
                with closing(conn.cursor()) as c:
                    c.execute('SELECT CHARSET(%s)', (MySQLdb.Binary(b'raw bytes'),))
                    self.assertEqual(c.fetchall()[0][0], 'binary' if binary_prefix else 'utf8')
                    # normal strings should not get prefix
                    c.execute('SELECT CHARSET(%s)', ('str',))
                    self.assertEqual(c.fetchall()[0][0], 'utf8')


if __name__ == '__main__':
    if test_MySQLdb.leak_test:
        import gc
        gc.enable()
        gc.set_debug(gc.DEBUG_LEAK)
    unittest.main()
