from datetime import date
import sqlalchemy as sa

from . import YqlDialect, types


def test_casts():
    dialect = YqlDialect()
    expr = sa.literal_column("1/2")

    res_exprs = [
        sa.cast(expr, types.UInt32),
        sa.cast(expr, types.UInt64),
        sa.cast(expr, types.UInt8),
        sa.func.String.JoinFromList(
            sa.func.ListMap(sa.func.TOPFREQ(expr, 5), types.Lambda(lambda x: sa.cast(x, sa.Text))),
            ", ",
        ),
    ]

    strs = [str(res_expr.compile(dialect=dialect, compile_kwargs={"literal_binds": True})) for res_expr in res_exprs]

    assert strs == [
        "CAST(1/2 AS UInt32)",
        "CAST(1/2 AS UInt64)",
        "CAST(1/2 AS UInt8)",
        "String::JoinFromList(ListMap(TOPFREQ(1/2, 5), ($x) -> { RETURN CAST($x AS UTF8) ;}), ', ')",
    ]


def test_ydb_types():
    dialect = YqlDialect()

    query = sa.literal(date(1996, 11, 19))
    compiled = query.compile(dialect=dialect, compile_kwargs={"literal_binds": True})

    assert str(compiled) == "Date('1996-11-19')"


def test_binary_type():
    dialect = YqlDialect()
    expr = sa.literal(b"some bytes")
    compiled = expr.compile(dialect=dialect, compile_kwargs={"literal_binds": True})
    assert str(compiled) == "'some bytes'"

    expr_binary = sa.cast(expr, sa.BINARY)
    compiled_binary = expr_binary.compile(dialect=dialect, compile_kwargs={"literal_binds": True})
    assert str(compiled_binary) == "CAST('some bytes' AS String)"


def test_all_binary_types():
    dialect = YqlDialect()
    expr = sa.literal(b"some bytes")

    binary_types = [
        sa.BINARY,
        sa.LargeBinary,
        sa.BLOB,
        types.Binary,
    ]

    for type_ in binary_types:
        expr_binary = sa.cast(expr, type_)
        compiled_binary = expr_binary.compile(dialect=dialect, compile_kwargs={"literal_binds": True})
        assert str(compiled_binary) == "CAST('some bytes' AS String)"


def test_struct_type_generation():
    dialect = YqlDialect()
    type_compiler = dialect.type_compiler

    # Test default (non-optional)
    struct_type = types.StructType(
        {
            "id": sa.Integer,
            "val_int": sa.Integer,
        }
    )
    ydb_type = type_compiler.get_ydb_type(struct_type, is_optional=False)
    # Keys are sorted
    assert str(ydb_type) == "Struct<id:Int64,val_int:Int64>"

    # Test optional
    struct_type_opt = types.StructType(
        {
            "id": sa.Integer,
            "val_int": types.Optional(sa.Integer),
        }
    )
    ydb_type_opt = type_compiler.get_ydb_type(struct_type_opt, is_optional=False)
    assert str(ydb_type_opt) == "Struct<id:Int64,val_int:Int64?>"


def test_types_compilation():
    dialect = YqlDialect()

    def compile_type(type_):
        return dialect.type_compiler.process(type_)

    assert compile_type(types.UInt64()) == "UInt64"
    assert compile_type(types.UInt32()) == "UInt32"
    assert compile_type(types.UInt16()) == "UInt16"
    assert compile_type(types.UInt8()) == "UInt8"

    assert compile_type(types.Int64()) == "Int64"
    assert compile_type(types.Int32()) == "Int32"
    assert compile_type(types.Int16()) == "Int32"
    assert compile_type(types.Int8()) == "Int8"

    assert compile_type(types.ListType(types.Int64())) == "List<Int64>"

    struct = types.StructType({"a": types.Int32(), "b": types.ListType(types.Int32())})
    # Ordered by key: a, b
    assert compile_type(struct) == "Struct<a:Int32,b:List<Int32>>"


def test_statement_prefixes_prepended_to_query():
    dialect = YqlDialect(_statement_prefixes_list=["PRAGMA DistinctOverKeys;"])
    result = dialect._apply_statement_prefixes_impl("SELECT 1")
    assert result == "PRAGMA DistinctOverKeys;\nSELECT 1"


def test_statement_prefixes_empty_list_unchanged():
    dialect = YqlDialect(_statement_prefixes_list=[])
    result = dialect._apply_statement_prefixes_impl("SELECT 1")
    assert result == "SELECT 1"


def test_statement_prefixes_none_unchanged():
    dialect = YqlDialect()
    result = dialect._apply_statement_prefixes_impl("SELECT 1")
    assert result == "SELECT 1"


def test_statement_prefixes_multiple():
    dialect = YqlDialect(_statement_prefixes_list=["PRAGMA Foo;", "PRAGMA Bar;"])
    result = dialect._apply_statement_prefixes_impl("SELECT 1")
    assert result == "PRAGMA Foo;\nPRAGMA Bar;\nSELECT 1"


def test_optional_type_compilation():
    dialect = YqlDialect()
    type_compiler = dialect.type_compiler

    def compile_type(type_):
        return type_compiler.process(type_)

    # Test Optional(Integer)
    opt_int = types.Optional(sa.Integer)
    assert compile_type(opt_int) == "Optional<Int64>"

    # Test Optional(String)
    opt_str = types.Optional(sa.String)
    assert compile_type(opt_str) == "Optional<UTF8>"

    # Test Nested Optional
    opt_opt_int = types.Optional(types.Optional(sa.Integer))
    assert compile_type(opt_opt_int) == "Optional<Optional<Int64>>"

    # Test get_ydb_type
    ydb_type = type_compiler.get_ydb_type(opt_int, is_optional=False)
    import ydb

    assert isinstance(ydb_type, ydb.OptionalType)
    # Int64 corresponds to PrimitiveType.Int64
    # Note: ydb.PrimitiveType.Int64 is an enum member, but ydb_type.item is also an instance/enum?
    # get_ydb_type returns ydb.PrimitiveType.Int64 (enum) wrapped in OptionalType.
    # OptionalType.item is the inner type.
    assert ydb_type.item == ydb.PrimitiveType.Int64
