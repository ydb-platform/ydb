# -*- coding: utf-8 -*-
import unittest

from infi.clickhouse_orm.database import Database
from infi.clickhouse_orm.models import Model
from infi.clickhouse_orm.fields import *
from infi.clickhouse_orm.engines import *

import logging
logging.getLogger("requests").setLevel(logging.WARNING)

from .common import get_clickhouse_url


class TestCaseWithData(unittest.TestCase):

    def setUp(self):
        self.database = Database('test-db', db_url=get_clickhouse_url(), log_statements=True)
        self.database.create_table(Person)

    def tearDown(self):
        self.database.drop_table(Person)
        self.database.drop_database()

    def _insert_all(self):
        self.database.insert(self._sample_data())
        self.assertTrue(self.database.count(Person))

    def _insert_and_check(self, data, count, batch_size=1000):
        self.database.insert(data, batch_size=batch_size)
        self.assertEqual(count, self.database.count(Person))
        for instance in data:
            self.assertEqual(self.database, instance.get_database())

    def _sample_data(self):
        for entry in data:
            yield Person(**entry)



class Person(Model):

    first_name = StringField()
    last_name = LowCardinalityField(StringField())
    birthday = DateField()
    height = Float32Field()
    passport = NullableField(UInt32Field())

    engine = MergeTree('birthday', ('first_name', 'last_name', 'birthday'))


data = [
    {"first_name": "Abdul", "last_name": "Hester", "birthday": "1970-12-02", "height": "1.63",
     "passport": 35052255},

    {"first_name": "Adam", "last_name": "Goodman", "birthday": "1986-01-07", "height": "1.74",
     "passport": 36052255},

    {"first_name": "Adena", "last_name": "Norman", "birthday": "1979-05-14", "height": "1.66"},
    {"first_name": "Aline", "last_name": "Crane", "birthday": "1988-05-01", "height": "1.62"},
    {"first_name": "Althea", "last_name": "Barrett", "birthday": "2004-07-28", "height": "1.71"},
    {"first_name": "Amanda", "last_name": "Vang", "birthday": "1973-02-23", "height": "1.68"},
    {"first_name": "Angela", "last_name": "Sanders", "birthday": "2016-01-08", "height": "1.66"},
    {"first_name": "Anne", "last_name": "Rasmussen", "birthday": "1995-04-03", "height": "1.77"},
    {"first_name": "Ariana", "last_name": "Cole", "birthday": "1977-12-20", "height": "1.72"},
    {"first_name": "Ashton", "last_name": "Fuller", "birthday": "1995-11-17", "height": "1.75"},
    {"first_name": "Ava", "last_name": "Sanders", "birthday": "1997-08-10", "height": "1.60"},
    {"first_name": "Barrett", "last_name": "Clemons", "birthday": "1985-07-03", "height": "1.71"},
    {"first_name": "Beatrice", "last_name": "Gregory", "birthday": "1992-01-19", "height": "1.80"},
    {"first_name": "Buffy", "last_name": "Webb", "birthday": "1990-03-06", "height": "1.68"},
    {"first_name": "Callie", "last_name": "Wiley", "birthday": "1987-11-24", "height": "1.69"},
    {"first_name": "Cara", "last_name": "Fox", "birthday": "2004-05-15", "height": "1.71"},
    {"first_name": "Caryn", "last_name": "Sears", "birthday": "1999-02-17", "height": "1.71"},
    {"first_name": "Cassady", "last_name": "Knapp", "birthday": "1977-12-15", "height": "1.72"},
    {"first_name": "Cassady", "last_name": "Rogers", "birthday": "2013-11-04", "height": "1.71"},
    {"first_name": "Catherine", "last_name": "Hicks", "birthday": "1989-05-23", "height": "1.80"},
    {"first_name": "Cathleen", "last_name": "Frank", "birthday": "1977-09-04", "height": "1.61"},
    {"first_name": "Celeste", "last_name": "James", "birthday": "1990-03-08", "height": "1.67"},
    {"first_name": "Chelsea", "last_name": "Castro", "birthday": "2001-08-10", "height": "1.71"},
    {"first_name": "Ciaran", "last_name": "Carver", "birthday": "2016-12-25", "height": "1.76"},
    {"first_name": "Ciaran", "last_name": "Hurley", "birthday": "1995-10-25", "height": "1.65"},
    {"first_name": "Clementine", "last_name": "Moon", "birthday": "1994-03-29", "height": "1.73"},
    {"first_name": "Connor", "last_name": "Jenkins", "birthday": "1999-07-23", "height": "1.67"},
    {"first_name": "Courtney", "last_name": "Cannon", "birthday": "1997-10-26", "height": "1.76"},
    {"first_name": "Courtney", "last_name": "Hoffman", "birthday": "1994-11-07", "height": "1.65"},
    {"first_name": "Denton", "last_name": "Sanchez", "birthday": "1971-10-16", "height": "1.72"},
    {"first_name": "Dominique", "last_name": "Sandoval", "birthday": "1972-02-01", "height": "1.72"},
    {"first_name": "Dora", "last_name": "Cabrera", "birthday": "2016-04-26", "height": "1.68"},
    {"first_name": "Eagan", "last_name": "Dodson", "birthday": "2015-10-22", "height": "1.67"},
    {"first_name": "Edan", "last_name": "Dennis", "birthday": "1989-09-18", "height": "1.73"},
    {"first_name": "Ella", "last_name": "Castillo", "birthday": "1973-03-28", "height": "1.73"},
    {"first_name": "Elton", "last_name": "Ayers", "birthday": "1994-06-20", "height": "1.68"},
    {"first_name": "Elton", "last_name": "Smith", "birthday": "1982-06-20", "height": "1.66"},
    {"first_name": "Emma", "last_name": "Clements", "birthday": "1996-08-07", "height": "1.75"},
    {"first_name": "Evangeline", "last_name": "Weber", "birthday": "1984-06-03", "height": "1.70"},
    {"first_name": "Faith", "last_name": "Emerson", "birthday": "1989-12-30", "height": "1.62"},
    {"first_name": "Fritz", "last_name": "Atkinson", "birthday": "2011-06-15", "height": "1.73"},
    {"first_name": "Galvin", "last_name": "Phillips", "birthday": "2004-01-17", "height": "1.74"},
    {"first_name": "Georgia", "last_name": "Kennedy", "birthday": "1974-12-29", "height": "1.66"},
    {"first_name": "Griffith", "last_name": "Henry", "birthday": "1985-04-02", "height": "1.66"},
    {"first_name": "Hedy", "last_name": "Strong", "birthday": "2001-10-04", "height": "1.60"},
    {"first_name": "Hu", "last_name": "May", "birthday": "1976-10-01", "height": "1.76"},
    {"first_name": "Hyacinth", "last_name": "Kent", "birthday": "1971-07-18", "height": "1.72"},
    {"first_name": "Idola", "last_name": "Fulton", "birthday": "1974-11-27", "height": "1.66"},
    {"first_name": "Jarrod", "last_name": "Gibbs", "birthday": "1987-06-13", "height": "1.62"},
    {"first_name": "Jesse", "last_name": "Gomez", "birthday": "2011-01-28", "height": "1.71"},
    {"first_name": "Josiah", "last_name": "Hodges", "birthday": "2011-09-04", "height": "1.68"},
    {"first_name": "Karleigh", "last_name": "Bartlett", "birthday": "1991-10-24", "height": "1.69"},
    {"first_name": "Keelie", "last_name": "Mathis", "birthday": "1993-10-26", "height": "1.69"},
    {"first_name": "Kieran", "last_name": "Solomon", "birthday": "1993-10-30", "height": "1.69"},
    {"first_name": "Laith", "last_name": "Howell", "birthday": "1991-07-07", "height": "1.70"},
    {"first_name": "Leroy", "last_name": "Pacheco", "birthday": "1998-12-30", "height": "1.70"},
    {"first_name": "Lesley", "last_name": "Stephenson", "birthday": "2010-04-10", "height": "1.64"},
    {"first_name": "Macaulay", "last_name": "Rowe", "birthday": "1982-03-02", "height": "1.68"},
    {"first_name": "Macey", "last_name": "Griffin", "birthday": "1971-09-18", "height": "1.63"},
    {"first_name": "Madeline", "last_name": "Kidd", "birthday": "1984-12-09", "height": "1.69"},
    {"first_name": "Maia", "last_name": "Hyde", "birthday": "1972-06-09", "height": "1.74"},
    {"first_name": "Mary", "last_name": "Kirkland", "birthday": "1987-10-09", "height": "1.73"},
    {"first_name": "Molly", "last_name": "Salas", "birthday": "1994-04-23", "height": "1.70"},
    {"first_name": "Montana", "last_name": "Bruce", "birthday": "1982-06-28", "height": "1.66"},
    {"first_name": "Naomi", "last_name": "Hays", "birthday": "2004-11-27", "height": "1.70"},
    {"first_name": "Norman", "last_name": "Santos", "birthday": "1989-01-10", "height": "1.68"},
    {"first_name": "Octavius", "last_name": "Floyd", "birthday": "1985-02-22", "height": "1.68"},
    {"first_name": "Odette", "last_name": "Mcneil", "birthday": "1978-05-21", "height": "1.76"},
    {"first_name": "Oliver", "last_name": "Ashley", "birthday": "2004-08-13", "height": "1.68"},
    {"first_name": "Quon", "last_name": "Wiggins", "birthday": "1992-05-06", "height": "1.74"},
    {"first_name": "Rafael", "last_name": "Parker", "birthday": "2016-01-24", "height": "1.76"},
    {"first_name": "Reese", "last_name": "Noel", "birthday": "1996-11-04", "height": "1.77"},
    {"first_name": "Rhona", "last_name": "Camacho", "birthday": "1976-12-17", "height": "1.59"},
    {"first_name": "Rigel", "last_name": "Oneal", "birthday": "1993-11-05", "height": "1.63"},
    {"first_name": "Roary", "last_name": "Simmons", "birthday": "1986-07-23", "height": "1.63"},
    {"first_name": "Russell", "last_name": "Pruitt", "birthday": "1979-05-04", "height": "1.63"},
    {"first_name": "Sawyer", "last_name": "Fischer", "birthday": "1995-04-01", "height": "1.78"},
    {"first_name": "Scarlett", "last_name": "Durham", "birthday": "2005-09-29", "height": "1.65"},
    {"first_name": "Seth", "last_name": "Serrano", "birthday": "2017-06-02", "height": "1.71"},
    {"first_name": "Shad", "last_name": "Bradshaw", "birthday": "1998-08-25", "height": "1.72"},
    {"first_name": "Shana", "last_name": "Jarvis", "birthday": "1997-05-21", "height": "1.72"},
    {"first_name": "Sharon", "last_name": "Shelton", "birthday": "1970-05-02", "height": "1.65"},
    {"first_name": "Shoshana", "last_name": "Solis", "birthday": "1998-07-18", "height": "1.65"},
    {"first_name": "Stephen", "last_name": "Baxter", "birthday": "2004-09-24", "height": "1.74"},
    {"first_name": "Sydney", "last_name": "Stevens", "birthday": "1989-07-11", "height": "1.70"},
    {"first_name": "Tasha", "last_name": "Campos", "birthday": "1984-02-11", "height": "1.72"},
    {"first_name": "Ulla", "last_name": "Arnold", "birthday": "1990-06-04", "height": "1.63"},
    {"first_name": "Vaughan", "last_name": "Schmidt", "birthday": "1985-06-19", "height": "1.61"},
    {"first_name": "Velma", "last_name": "English", "birthday": "1999-01-18", "height": "1.65"},
    {"first_name": "Venus", "last_name": "Hurst", "birthday": "1993-10-22", "height": "1.72"},
    {"first_name": "Victor", "last_name": "Woods", "birthday": "1989-06-23", "height": "1.67"},
    {"first_name": "Victoria", "last_name": "Slater", "birthday": "2009-07-19", "height": "1.72"},
    {"first_name": "Wang", "last_name": "Goodwin", "birthday": "1983-05-15", "height": "1.66"},
    {"first_name": "Warren", "last_name": "Bowen", "birthday": "2000-07-20", "height": "1.76"},
    {"first_name": "Warren", "last_name": "Dudley", "birthday": "1995-10-23", "height": "1.59"},
    {"first_name": "Whilemina", "last_name": "Blankenship", "birthday": "1970-07-14", "height": "1.66"},
    {"first_name": "Whitney", "last_name": "Durham", "birthday": "1977-09-15", "height": "1.72"},
    {"first_name": "Whitney", "last_name": "Scott", "birthday": "1971-07-04", "height": "1.70"},
    {"first_name": "Wynter", "last_name": "Garcia", "birthday": "1975-01-10", "height": "1.69"},
    {"first_name": "Yolanda", "last_name": "Duke", "birthday": "1997-02-25", "height": "1.74"}
]
