# Generated from /home/runner/work/OpenMetadata/OpenMetadata/openmetadata-spec/src/main/antlr4/org/openmetadata/schema/JdbcUri.g4 by ANTLR 4.9.2
from antlr4 import *
if __name__ is not None and "." in __name__:
    from .JdbcUriParser import JdbcUriParser
else:
    from JdbcUriParser import JdbcUriParser

# This class defines a complete listener for a parse tree produced by JdbcUriParser.
class JdbcUriListener(ParseTreeListener):

    # Enter a parse tree produced by JdbcUriParser#jdbcUrl.
    def enterJdbcUrl(self, ctx:JdbcUriParser.JdbcUrlContext):
        pass

    # Exit a parse tree produced by JdbcUriParser#jdbcUrl.
    def exitJdbcUrl(self, ctx:JdbcUriParser.JdbcUrlContext):
        pass


    # Enter a parse tree produced by JdbcUriParser#schemaTable.
    def enterSchemaTable(self, ctx:JdbcUriParser.SchemaTableContext):
        pass

    # Exit a parse tree produced by JdbcUriParser#schemaTable.
    def exitSchemaTable(self, ctx:JdbcUriParser.SchemaTableContext):
        pass


    # Enter a parse tree produced by JdbcUriParser#databaseName.
    def enterDatabaseName(self, ctx:JdbcUriParser.DatabaseNameContext):
        pass

    # Exit a parse tree produced by JdbcUriParser#databaseName.
    def exitDatabaseName(self, ctx:JdbcUriParser.DatabaseNameContext):
        pass


    # Enter a parse tree produced by JdbcUriParser#schemaName.
    def enterSchemaName(self, ctx:JdbcUriParser.SchemaNameContext):
        pass

    # Exit a parse tree produced by JdbcUriParser#schemaName.
    def exitSchemaName(self, ctx:JdbcUriParser.SchemaNameContext):
        pass


    # Enter a parse tree produced by JdbcUriParser#tableName.
    def enterTableName(self, ctx:JdbcUriParser.TableNameContext):
        pass

    # Exit a parse tree produced by JdbcUriParser#tableName.
    def exitTableName(self, ctx:JdbcUriParser.TableNameContext):
        pass


    # Enter a parse tree produced by JdbcUriParser#serverName.
    def enterServerName(self, ctx:JdbcUriParser.ServerNameContext):
        pass

    # Exit a parse tree produced by JdbcUriParser#serverName.
    def exitServerName(self, ctx:JdbcUriParser.ServerNameContext):
        pass



del JdbcUriParser