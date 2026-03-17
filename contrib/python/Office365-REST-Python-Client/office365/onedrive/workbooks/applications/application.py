from office365.entity import Entity
from office365.runtime.queries.service_operation import ServiceOperationQuery


class WorkbookApplication(Entity):
    """Represents the Excel application that manages the workbook."""

    def calculate(self, calculation_type=None):
        """Recalculate all currently opened workbooks in Excel.

        :param str calculation_type: Specifies the calculation type to use. Possible values are: Recalculate, Full,
            FullRebuild.
        """
        payload = {"calculationType": calculation_type}
        qry = ServiceOperationQuery(self, "calculate", None, payload)
        self.context.add_query(qry)
        return self

    @property
    def calculation_mode(self):
        """Returns the calculation mode used in the workbook. Possible values are:
        Automatic, AutomaticExceptTables, Manual."""
        return self.properties.get("calculationMode", None)
