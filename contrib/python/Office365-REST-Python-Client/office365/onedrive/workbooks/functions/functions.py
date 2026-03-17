from office365.entity import Entity
from office365.onedrive.workbooks.functions.result import WorkbookFunctionResult
from office365.runtime.queries.service_operation import ServiceOperationQuery


class WorkbookFunctions(Entity):
    """Used as a container for Microsoft Excel worksheet function"""

    def abs(self, number):
        """
        Returns the absolute value of a number. The absolute value of a number is the number without its sign
        :param float number: The real number of which you want the absolute value.
        """
        return_type = WorkbookFunctionResult(self.context)
        payload = {
            "number": number,
        }
        qry = ServiceOperationQuery(self, "abs", None, payload, None, return_type)
        self.context.add_query(qry)
        return return_type

    def accr_int(self, issue, first_interest, settlement, rate, par, frequency):
        """
        Returns the accrued interest for a security that pays periodic interest.

        :param any issue: The real number of which you want the absolute value.
        :param any first_interest: The security's first interest date.
        :param any settlement: The security's settlement date. The security settlement date is the date after
            the issue date when the security is traded to the buyer.
        :param any rate: The security's annual coupon rate.
        :param any par: The security's par value. If you omit par, ACCRINT uses $1,000.
        :param any frequency: The number of coupon payments per year.
            For annual payments, frequency = 1; for semiannual, frequency = 2; for quarterly, frequency = 4.
        """
        return_type = WorkbookFunctionResult(self.context)
        payload = {
            "issue": issue,
            "firstInterest": first_interest,
            "settlement": settlement,
            "rate": rate,
            "par": par,
            "frequency": frequency,
        }
        qry = ServiceOperationQuery(self, "accrInt", None, payload, None, return_type)
        self.context.add_query(qry)
        return return_type

    def accr_int_m(self, issue, settlement, rate, par, basis):
        """
        Returns the accrued interest for a security that pays periodic interest.

        :param any issue: The real number of which you want the absolute value.
        :param any settlement: The security's settlement date. The security settlement date is the date after
            the issue date when the security is traded to the buyer.
        :param any rate: The security's annual coupon rate.
        :param any par: The security's par value. If you omit par, ACCRINT uses $1,000.
        :param any basis:
        """
        return_type = WorkbookFunctionResult(self.context)
        payload = {
            "issue": issue,
            "settlement": settlement,
            "rate": rate,
            "par": par,
            "basis": basis,
        }
        qry = ServiceOperationQuery(self, "accrIntM", None, payload, None, return_type)
        self.context.add_query(qry)
        return return_type

    def days(self, start_date, end_date):
        """Returns the number of days between two dates.

        :param datetime.datetime start_date: Two dates between which you want to know the number of days.
        :param datetime.datetime end_date: Two dates between which you want to know the number of days.
        """
        return_type = WorkbookFunctionResult(self.context)
        payload = {
            "startDate": start_date.isoformat() + "Z",
            "endDate": end_date.isoformat() + "Z",
        }
        qry = ServiceOperationQuery(self, "days", None, payload, None, return_type)
        self.context.add_query(qry)
        return return_type

    def power(self, number, power):
        """
        Returns the result of a number raised to a power.

        :param int number: The base number.
        :param int power: The exponent to which the base number is raised.
        """
        return_type = WorkbookFunctionResult(self.context)
        payload = {
            "number": number,
            "power": power,
        }
        qry = ServiceOperationQuery(self, "power", None, payload, None, return_type)
        self.context.add_query(qry)
        return return_type
