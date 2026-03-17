[![Documentation Status](https://readthedocs.org/projects/asyncpgsa/badge/?version=latest)](http://asyncpgsa.readthedocs.io/en/latest/?badge=latest)

# asyncpgsa
A python library wrapper around asyncpg for use with sqlalchemy

## Backwards incompatibility notice
Since this library is still in pre 1.0 world, the api might change. 
I will do my best to minimize changes, and any changes that get added, 
I will mention here. You should lock the version for production apps.

1. 0.9.0 changed the dialect from psycopg2 to pypostgres. This should be
mostly backwards compatible, but if you notice weird issues, this is why.
You can now plug-in your own dialect using `pg.init(..., dialect=my_dialect)`,
or setting the dialect on the pool. See the top of the connection file 
for an example of creating a dialect. Please let me know if the change from
psycopg2 to pypostgres broke you. If this happens enough, 
I might make psycopg2 the default.

2. 0.18.0 Removes the Record Proxy objects that would wrap asyncpg's records. Now
asyncpgsa just returns whatever asyncpg would return. This is a HUGE backwards incompatible change
but most people just used record._data to get the object directly anyways. This means dot notation
for columns is no longer possible and you need to access columns using exact names with dictionary notation.

3. 0.18.0 Removed the `insert` method. We found this method was just confusing, and useless as SqlAlchemy can do it for you by defining your table with a primary key.

4. 0.27.0 Now only compatible with version 0.22.0 and greater of asyncpg.

## sqlalchemy ORM

Currently this repo does not support SA ORM, only SA Core.

As we at canopy do not use the ORM, if you would like to have ORM support
feel free to PR it. You would need to create an "engine" interface, and that
should be it. Then you can bind your sessions to the engine.


## sqlalchemy Core

This repo supports sqlalchemy core. Go [here](https://github.com/CanopyTax/asyncpgsa/wiki/Examples) for examples.

## Docs

Go [here](https://asyncpgsa.readthedocs.io/en/latest/) for docs.

## Examples
Go [here](https://github.com/CanopyTax/asyncpgsa/wiki/Examples) for examples.

## install

```bash
pip install asyncpgsa
```
**Note**: You should **not** have `asyncpg` in your requirements at all. This lib will pull down the correct version of asyncpg for you. If you have asyncpg in your requirements, you could get a version newer than this one supports.

## Contributing
To contribute or build this locally see [contributing.md](https://github.com/CanopyTax/asyncpgsa/blob/master/contributing.md)


## FAQ

#### Does SQLAlchemy integration defeat the point of using asyncpg as a backend (performance)?
I dont think so. `asyncpgsa` is written in a way where any query can be a string instead of an SA object, then you will get near asyncpg speeds, as no SA code is ran. 

However, when running SA queries, comparing this to `aiopg`, it still seams to work faster. Here is a very basic `timeit` test comparing the two.
https://gist.github.com/nhumrich/3470f075ae1d868f663b162d01a07838

```
aiopg.sa: 9.541276566000306
asyncpsa: 6.747777451004367
```
So, seems like its still faster using asyncpg, or in otherwords, this library doesnt add any overhead that is not in aiopg.sa.

## Versioning

This software follows [Semantic Versioning](http://semver.org/).
