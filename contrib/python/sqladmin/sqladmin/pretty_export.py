from typing import TYPE_CHECKING, Any, AsyncGenerator, List

from starlette.responses import StreamingResponse

from sqladmin.helpers import Writer, secure_filename, stream_to_csv

if TYPE_CHECKING:
    from .models import ModelView


class PrettyExport:
    @staticmethod
    async def _base_export_cell(
        model_view: "ModelView", name: str, value: Any, formatted_value: Any
    ) -> str:
        """
        Default formatting logic for a cell in pretty export.

        Used when `custom_export_cell` returns None.
        Applies standard rules for related fields, booleans, etc.

        Only used when `use_pretty_export = True`.
        """
        if name in model_view._relation_names:
            if isinstance(value, list):
                cell_value = ",".join(formatted_value)
            else:
                cell_value = formatted_value
        else:
            if isinstance(value, bool):
                cell_value = "TRUE" if value else "FALSE"
            else:
                cell_value = formatted_value
        return cell_value

    @classmethod
    async def _get_export_row_values(
        cls, model_view: "ModelView", row: Any, column_names: List[str]
    ) -> List[Any]:
        row_values = []
        for name in column_names:
            value, formatted_value = await model_view.get_list_value(row, name)
            custom_value = await model_view.custom_export_cell(row, name, value)
            if custom_value is None:
                cell_value = await cls._base_export_cell(
                    model_view, name, value, formatted_value
                )
            else:
                cell_value = custom_value
            row_values.append(cell_value)
        return row_values

    @classmethod
    async def pretty_export_csv(
        cls, model_view: "ModelView", rows: List[Any]
    ) -> StreamingResponse:
        async def generate(writer: Writer) -> AsyncGenerator[Any, None]:
            column_names = model_view.get_export_columns()
            headers = [
                model_view._column_labels.get(name, name) for name in column_names
            ]

            yield writer.writerow(headers)

            for row in rows:
                vals = await cls._get_export_row_values(model_view, row, column_names)
                yield writer.writerow(vals)

        filename = secure_filename(model_view.get_export_name(export_type="csv"))

        return StreamingResponse(
            content=stream_to_csv(generate),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment;filename={filename}"},
        )
