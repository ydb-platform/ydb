class ResourceOptions:
    """
    The inner Meta class allows for class-level configuration of how the
    Resource should behave. The following options are available:
    """

    model = None
    """
    Django Model class or full application label string. It is used to introspect
    available fields.

    """
    fields = None
    """
    Controls what introspected fields the Resource should include. A whitelist
    of fields.
    """

    exclude = None
    """
    Controls what introspected fields the Resource should
    NOT include. A blacklist of fields.
    """

    instance_loader_class = None
    """
    Controls which class instance will take
    care of loading existing objects.
    """

    import_id_fields = ["id"]
    """
    Controls which object fields will be used to
    identify existing instances.
    """

    import_order = None
    """
    Controls import order for columns.
    """

    export_order = None
    """
    Controls export order for columns.
    """

    widgets = None
    """
    This dictionary defines widget kwargs for fields.
    """

    use_transactions = None
    """
    Controls if import should use database transactions. Default value is
    ``None`` meaning ``settings.IMPORT_EXPORT_USE_TRANSACTIONS`` will be
    evaluated.
    """

    skip_unchanged = False
    """
    Controls if the import should skip unchanged records.
    If ``True``, then each existing instance is compared with the instance to be
    imported, and if there are no changes detected, the row is recorded as skipped,
    and no database update takes place.

    The advantages of enabling this option are:

    #. Avoids unnecessary database operations which can result in performance
       improvements for large datasets.

    #. Skipped records are recorded in each :class:`~import_export.results.RowResult`.

    #. Skipped records are clearly visible in the
       :ref:`import confirmation page<import-process>`.

    For the default ``skip_unchanged`` logic to work, the
    :attr:`~import_export.resources.ResourceOptions.skip_diff` must also be ``False``
    (which is the default):

    Default value is ``False``.
    """

    report_skipped = True
    """
    Controls if the result reports skipped rows. Default value is ``True``.
    """

    clean_model_instances = False
    """
    Controls whether
    `full_clean <https://docs.djangoproject.com/en/stable/ref/models/instances/#django.db.models.Model.full_clean>`_
    is called during the import
    process to identify potential validation errors for each (non skipped) row.
    The default value is ``False``.
    """  # noqa: E501

    chunk_size = None
    """
    Controls the chunk_size argument of Queryset.iterator or,
    if prefetch_related is used, the per_page attribute of Paginator.
    """

    skip_diff = False
    """
    Controls whether or not an instance should be diffed following import.

    By default, an instance is copied prior to insert, update or delete.
    After each row is processed, the instance's copy is diffed against the original,
    and the value stored in each :class:`~import_export.results.RowResult`.
    If diffing is not required, then disabling the diff operation by setting this value
    to ``True`` improves performance, because the copy and comparison operations are
    skipped for each row.

    If enabled, then :meth:`~import_export.resources.Resource.skip_row` checks do not
    execute, because 'skip' logic requires comparison between the stored and imported
    versions of a row.

    If enabled, then HTML row reports are also not generated, meaning that the
    :attr:`~import_export.resources.ResourceOptions.skip_html_diff` value is ignored.

    The default value is ``False``.
    """

    skip_html_diff = False
    """
    Controls whether or not a HTML report is generated after each row.
    By default, the difference between a stored copy and an imported instance
    is generated in HTML form and stored in each
    :class:`~import_export.results.RowResult`.

    The HTML report is used to present changes in the
    :ref:`import confirmation page<import-process>` in the admin site, hence when this
    value is ``True``, then changes will not be presented on the confirmation screen.

    If the HTML report is not required, then setting this value to ``True`` improves
    performance, because the HTML generation is skipped for each row.
    This is a useful optimization when importing large datasets.

    The default value is ``False``.
    """

    use_bulk = False
    """
    Controls whether import operations should be performed in bulk.
    By default, an object's save() method is called for each row in a data set.
    When bulk is enabled, objects are saved using bulk operations.
    """

    batch_size = 1000
    """
    The batch_size parameter controls how many objects are created in a single query.
    The default is to create objects in batches of 1000.
    See `bulk_create()
    <https://docs.djangoproject.com/en/dev/ref/models/querysets/#bulk-create>`_.
    This parameter is only used if ``use_bulk`` is ``True``.
    """

    force_init_instance = False
    """
    If ``True``, this parameter will prevent imports from checking the database for
    existing instances.
    Enabling this parameter is a performance enhancement if your import dataset is
    guaranteed to contain new instances.
    """

    using_db = None
    """
    DB Connection name to use for db transactions. If not provided,
    ``router.db_for_write(model)`` will be evaluated and if it's missing,
    ``DEFAULT_DB_ALIAS`` constant ("default") is used.
    """

    store_row_values = False
    """
    If True, each row's raw data will be stored in each
    :class:`~import_export.results.RowResult`.
    Enabling this parameter will increase the memory usage during import
    which should be considered when importing large datasets.
    """

    store_instance = False
    """
    If True, the row instance will be stored in each
    :class:`~import_export.results.RowResult`.
    Enabling this parameter will increase the memory usage during import
    which should be considered when importing large datasets.

    This value will always be set to ``True`` when importing via the Admin UI.
    This is so that appropriate ``LogEntry`` instances can be created.
    """

    use_natural_foreign_keys = False
    """
    If ``True``, this value will be passed to all foreign
    key widget fields whose models support natural foreign keys. That is,
    the model has a natural_key function and the manager has a
    ``get_by_natural_key()`` function.
    """
