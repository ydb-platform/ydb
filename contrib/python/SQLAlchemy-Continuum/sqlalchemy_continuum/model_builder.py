from copy import copy

import sqlalchemy as sa
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm import column_property
from ._compat import get_declarative_base

from .utils import adapt_columns, option
from .version import VersionClassBase


def find_closest_versioned_parent(manager, model):
    """
    Finds the closest versioned parent for current parent model.
    """
    for class_ in model.__bases__:
        if class_ in manager.version_class_map:
            return manager.version_class_map[class_]


def versioned_parents(manager, model):
    """
    Finds all versioned ancestors for current parent model.
    """
    for class_ in model.__mro__:
        if class_ in manager.version_class_map:
            yield manager.version_class_map[class_]


def get_base_class(manager, model):
    """
    Returns all base classes for history model.
    """
    return option(model, 'base_classes') or (get_declarative_base(model),)


def version_base(manager, parent_cls, base_class_factory=None):
    if base_class_factory is None:
        base_class_factory = get_base_class

    VersionBase = find_closest_versioned_parent(manager, parent_cls)

    if not VersionBase:
        VersionBase = type(
            'VersionBase',
            (base_class_factory(manager, parent_cls) + (VersionClassBase,)),
            {'__abstract__': True},
        )

    return VersionBase


def copy_mapper_args(model):
    args = {}
    if hasattr(model, '__mapper_args__'):
        arg_names = ('with_polymorphic', 'polymorphic_identity', 'concrete')
        for arg in arg_names:
            if arg in model.__mapper_args__:
                args[arg] = model.__mapper_args__[arg]

        if 'order_by' in model.__mapper_args__:
            arg = model.__mapper_args__['order_by']
            # Only allow string based order_by reflection to version
            # classes.
            if isinstance(arg, str):
                args['order_by'] = arg

        if 'polymorphic_on' in model.__mapper_args__:
            column = model.__mapper_args__['polymorphic_on']
            if isinstance(column, str):
                args['polymorphic_on'] = column
            else:
                args['polymorphic_on'] = column.key
    return args


class ModelBuilder:
    """
    VersionedModelBuilder handles the building of Version models based on
    parent table attributes and versioning configuration.
    """

    def __init__(self, versioning_manager, model):
        """
        :param versioning_manager:
            VersioningManager object
        :param model:
            SQLAlchemy declarative model object that acts as a parent for the
            built version model
        """
        self.manager = versioning_manager
        self.model = model

    def build_parent_relationship(self):
        """
        Builds a relationship between currently built version class and
        parent class (the model whose history the currently build version
        class represents).
        """
        conditions = []
        foreign_keys = []
        model_keys = []
        for key, column in sa.inspect(self.model).columns.items():
            if column.primary_key:
                conditions.append(
                    getattr(self.model, key) == getattr(self.version_class, key)
                )
                foreign_keys.append(getattr(self.version_class, key))
                model_keys.append(getattr(self.model, key))

        # We need to check if versions relation was already set for parent
        # class.
        if not hasattr(self.model, 'versions'):
            self.model.versions = sa.orm.relationship(
                self.version_class,
                primaryjoin=sa.and_(*conditions),
                foreign_keys=foreign_keys,
                order_by=lambda: getattr(
                    self.version_class, option(self.model, 'transaction_column_name')
                ),
                lazy='dynamic',
                viewonly=True,
            )
            # We must explicitly declare this relationship, instead of
            # specifying as a backref to the one above, since they are
            # viewonly=True and SQLAlchemy will warn if using backref.
            self.version_class.version_parent = sa.orm.relationship(
                self.model,
                primaryjoin=sa.and_(*conditions),
                foreign_keys=model_keys,
                viewonly=True,
                uselist=False,
            )

    def build_transaction_relationship(self, tx_class):
        """
        Builds a relationship between currently built version class and
        Transaction class.

        :param tx_class: Transaction class
        """
        # Only define transaction relation if it doesn't already exist in
        # parent class.

        transaction_column = getattr(
            self.version_class, option(self.model, 'transaction_column_name')
        )

        if not hasattr(self.version_class, 'transaction'):
            self.version_class.transaction = sa.orm.relationship(
                tx_class,
                primaryjoin=tx_class.id == transaction_column,
                foreign_keys=[transaction_column],
            )

    def base_classes(self):
        """
        Returns all base classes for history model.
        """
        return (version_base(self.manager, self.model),)

    def inheritance_args(self, cls, version_table, table):
        """
        Return mapper inheritance args for currently built history model.
        """
        args = {}

        if not sa.inspect(self.model).single:
            parent = find_closest_versioned_parent(self.manager, self.model)
            if parent:
                # The version classes do not contain foreign keys, hence we
                # need to map inheritance condition manually for classes that
                # use joined table inheritance
                if parent.__table__.name != table.name:
                    mapper = sa.inspect(self.model)

                    inherit_condition = adapt_columns(mapper.inherit_condition)
                    tx_column_name = self.manager.options['transaction_column_name']
                    args['inherit_condition'] = sa.and_(
                        inherit_condition,
                        getattr(parent.__table__.c, tx_column_name)
                        == getattr(cls.__table__.c, tx_column_name),
                    )
                    args['inherit_foreign_keys'] = [
                        version_table.c[column.key]
                        for column in sa.inspect(self.model).columns
                        if column.primary_key
                    ]

        args.update(copy_mapper_args(self.model))

        return args

    def get_inherited_denormalized_columns(self, table):
        parent_models = list(versioned_parents(self.manager, self.model))
        mapper = sa.inspect(self.model)
        args = {}

        if parent_models and not (mapper.single or mapper.concrete):
            columns = [
                self.manager.option(self.model, 'operation_type_column_name'),
                self.manager.option(self.model, 'transaction_column_name'),
            ]
            if self.manager.option(self.model, 'strategy') == 'validity':
                columns.append(
                    self.manager.option(self.model, 'end_transaction_column_name')
                )

            for column in columns:
                args[column] = column_property(
                    table.c[column], *[m.__table__.c[column] for m in parent_models]
                )
        return args

    def build_model(self, table):
        """
        Build history model class.
        """
        args = {}

        @declared_attr
        def mapper_args(cls):
            mapper_args = {}
            mapper_args.update(self.inheritance_args(cls, table, self.model.__table__))
            return mapper_args

        args['__mapper_args__'] = mapper_args
        args['__versioning_manager__'] = self.manager
        args['__version_parent__'] = self.model

        parent = find_closest_versioned_parent(self.manager, self.model)

        if not parent or parent.__table__.name != table.name:
            args['__table__'] = table

        args.update(self.get_inherited_denormalized_columns(table))

        if self.manager.options.get('use_module_name', True):
            name = '{}{}Version'.format(
                self.model.__module__.title().replace('.', ''),
                self.model.__name__,
            )
        else:
            name = f'{self.model.__name__}Version'
        return type(name, self.base_classes(), args)

    def __call__(self, table, tx_class):
        """
        Build history model and relationships to parent model, transaction
        log model.
        """
        # versioned attributes need to be copied for each child class,
        # otherwise each child class would share the same __versioned__
        # option dict
        self.model.__versioned__ = copy(self.model.__versioned__)
        self.model.__versioning_manager__ = self.manager
        self.version_class = self.build_model(table)
        self.build_parent_relationship()
        self.build_transaction_relationship(tx_class)
        return self.version_class
