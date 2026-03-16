from sqlalchemy.testing import exclusions
from sqlalchemy.testing.requirements import SuiteRequirements


class Requirements(SuiteRequirements):
    @property
    def json_type(self):
        return exclusions.open()

    @property
    def array_type(self):
        return exclusions.closed()

    @property
    def uuid_data_type(self):
        return exclusions.open()

    @property
    def foreign_keys(self):
        # foreign keys unsupported
        return exclusions.closed()

    @property
    def self_referential_foreign_keys(self):
        return exclusions.closed()

    @property
    def foreign_key_ddl(self):
        return exclusions.closed()

    @property
    def foreign_key_constraint_reflection(self):
        return exclusions.closed()

    @property
    def temp_table_reflection(self):
        return exclusions.closed()

    @property
    def temporary_tables(self):
        return exclusions.closed()

    @property
    def temporary_views(self):
        return exclusions.closed()

    @property
    def index_reflection(self):
        # Reflection supported with limits
        return exclusions.closed()

    @property
    def view_reflection(self):
        return exclusions.closed()

    @property
    def unique_constraint_reflection(self):
        return exclusions.closed()

    @property
    def insert_returning(self):
        return exclusions.closed()

    @property
    def autoincrement_insert(self):
        # YDB doesn't support autoincrement
        return exclusions.closed()

    @property
    def autoincrement_without_sequence(self):
        # YDB doesn't support autoincrement
        return exclusions.closed()

    @property
    def duplicate_names_in_cursor_description(self):
        return exclusions.closed()

    @property
    def regexp_match(self):
        return exclusions.open()

    @property
    def table_value_constructor(self):
        return exclusions.open()

    @property
    def named_constraints(self):
        return exclusions.closed()

    @property
    def timestamp_microseconds(self):
        return exclusions.open()

    @property
    def mod_operator_as_percent_sign(self):
        return exclusions.open()

    @property
    def parens_in_union_contained_select_w_limit_offset(self):
        """union with brackets don't work"""
        return exclusions.closed()

    @property
    def parens_in_union_contained_select_wo_limit_offset(self):
        """union with brackets don't work"""
        return exclusions.closed()
