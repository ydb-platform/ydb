from .export import TableExport


class ExportMixin:
    """
    Support various export formats for the table data.

    `ExportMixin` looks for some attributes on the class to change it's behavior:

    Attributes:
        export_class (TableExport): Allows using a custom implementation of `TableExport`.
        export_name (str): is the name of file that will be exported, without extension.
        export_trigger_param (str): is the name of the GET attribute used to trigger
            the export. It's value decides the export format, refer to
            `TableExport` for a list of available formats.
        exclude_columns (iterable): column names excluded from the export.
            For example, one might want to exclude columns containing buttons from
            the export. Excluding columns from the export is also possible using the
            `exclude_from_export` argument to the `.Column` constructor::

                class Table(tables.Table):
                    name = tables.Column()
                    buttons = tables.TemplateColumn(exclude_from_export=True, template_name=...)
        export_formats (iterable): export formats to render a set of buttons in the template.
        dataset_kwargs (dictionary): passed as `**kwargs` to `tablib.Dataset` constructor::

            dataset_kwargs = {"title": "My custom tab title"}
    """

    export_class = TableExport
    export_name = "table"
    export_trigger_param = "_export"
    exclude_columns = ()
    dataset_kwargs = None

    export_formats = (TableExport.CSV,)

    def get_export_filename(self, export_format):
        return f"{self.export_name}.{export_format}"

    def get_dataset_kwargs(self):
        return self.dataset_kwargs

    def create_export(self, export_format):
        exporter = self.export_class(
            export_format=export_format,
            table=self.get_table(**self.get_table_kwargs()),
            exclude_columns=self.exclude_columns,
            dataset_kwargs=self.get_dataset_kwargs(),
        )

        return exporter.response(filename=self.get_export_filename(export_format))

    def render_to_response(self, context, **kwargs):
        export_format = self.request.GET.get(self.export_trigger_param, None)
        if self.export_class.is_valid_format(export_format):
            return self.create_export(export_format)

        return super().render_to_response(context, **kwargs)
