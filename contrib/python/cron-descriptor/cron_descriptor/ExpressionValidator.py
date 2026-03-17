from __future__ import annotations

import re
from typing import ClassVar

from cron_descriptor import FormatError


class ExpressionValidator:
    _cron_days: ClassVar[dict[int, str]] = {
        0: "SUN",
        1: "MON",
        2: "TUE",
        3: "WED",
        4: "THU",
        5: "FRI",
        6: "SAT",
    }

    _cron_months: ClassVar[dict[int, str]] = {
        1: "JAN",
        2: "FEB",
        3: "MAR",
        4: "APR",
        5: "MAY",
        6: "JUN",
        7: "JUL",
        8: "AUG",
        9: "SEP",
        10: "OCT",
        11: "NOV",
        12: "DEC",
    }

    def validate(self, expression: str) -> None:
        """Parses the cron expression string
        Returns:
            A 7 part string array, one part for each component of the cron expression (seconds, minutes, etc.)

        Raises:
            MissingFieldException: if _expression is empty or None
            FormatException: if _expression has wrong format

        """
        # Initialize all elements of parsed array to empty strings
        parsed = ["", "", "", "", "", "", ""]


        expression_parts_temp = expression.split()
        expression_parts_temp_length = len(expression_parts_temp)
        if expression_parts_temp_length < 5:
            msg = f"Error: Expression only has {expression_parts_temp_length} parts.  At least 5 part are required."
            raise FormatError(msg)
        if expression_parts_temp_length == 5:
            # 5 part cron so shift array past seconds element
            for i, expression_part_temp in enumerate(expression_parts_temp):
                parsed[i + 1] = expression_part_temp
        elif expression_parts_temp_length == 6:
            # We will detect if this 6 part expression has a year specified and if so we will shift the parts and treat the
            # first part as a minute part rather than a second part.
            # Ways we detect:
            # 1. Last part is a literal year (i.e. 2020)
            # 2. 3rd or 5th part is specified as "?" (DOM or DOW)
            year_regex = re.compile(r"\d{4}$")
            is_year_with_no_seconds_part = bool(year_regex.search(expression_parts_temp[5])) or "?" in [
                expression_parts_temp[4], expression_parts_temp[2]]
            for i, expression_part_temp in enumerate(expression_parts_temp):
                if is_year_with_no_seconds_part:
                    # Shift parts over by one
                    parsed[i + 1] = expression_part_temp
                else:
                    parsed[i] = expression_part_temp

        elif expression_parts_temp_length == 7:
            parsed = expression_parts_temp
        else:
            msg = f"Error: Expression has too many parts ({expression_parts_temp_length}).  Expression must not have more than 7 parts."
            raise FormatError(msg)

        self._validate_expression(parsed, expression_parts_temp_length)

    def _validate_expression(self, expression_parts: list[str], expr_length: int) -> None:
        """Validation for each expression fields
        Args:
            expression_parts: expression list
            expr_length: length of the list
        """

        """
        Apply different index for varying length of the expression parts as it is mutated by parse().
        Does not validate the case for having both DOW,DOM value because it is already causing exception.
        """
        if expr_length == 5:
            self.second_minute(expression_parts[1], "Second and Minute")
            self.hour(expression_parts[2], "Hour")
            self.dayofmonth(expression_parts[3], "DayOfMonth")
            self.month(expression_parts[4], "Month")
            self.dayofweek(expression_parts[5], "DayOfWeek")
        elif expr_length == 6:
            year_regex = re.compile(r"\d{4}$")
            if year_regex.search(expression_parts[6]) is None:
                if expression_parts[0]:
                    self.second_minute(expression_parts[0], "Second and Minute")
                self.second_minute(expression_parts[1], "Second and Minute")
                self.hour(expression_parts[2], "Hour")
                self.dayofmonth(expression_parts[3], "DayOfMonth")
                self.month(expression_parts[4], "Month")
                self.dayofweek(expression_parts[5], "DayOfWeek")
            else:
                self.second_minute(expression_parts[1], "Second and Minute")
                self.hour(expression_parts[2], "Hour")
                self.dayofmonth(expression_parts[3], "DayOfMonth")
                self.month(expression_parts[4], "Month")
                self.dayofweek(expression_parts[5], "DayOfWeek")
                self.year(expression_parts[6], "Year")
        else:
            if expression_parts[0]:
                self.second_minute(expression_parts[0], "Second and Minute")
            self.second_minute(expression_parts[1], "Second and Minute")
            self.hour(expression_parts[2], "Hour")
            self.dayofmonth(expression_parts[3], "DayOfMonth")
            self.month(expression_parts[4], "Month")
            self.dayofweek(expression_parts[5], "DayOfWeek")
            if expression_parts[6]:
                self.year(expression_parts[6], "Year")

    def second_minute(self, expr: str, prefix: str) -> None:
        """ sec/min expressions (n : Number, s: String)
        *
        nn (1~59)
        nn-nn
        nn/nn
        nn-nn/nn
        */nn
        nn,nn,nn (Maximum 24 elements)
        """
        mi, mx = (0, 59)
        if re.match(r"\d{1,2}$", expr):
            self.check_range(expr=expr, mi=mi, mx=mx, prefix=prefix)

        elif re.search(r"[-*,/]", expr):
            if expr == "*":
                pass

            elif re.match(r"\d{1,2}-\d{1,2}$", expr):
                parts = expr.split("-")
                self.check_range(expr=parts[0], mi=mi, mx=mx, prefix=prefix)
                self.check_range(expr=parts[1], mi=mi, mx=mx, prefix=prefix)
                self.compare_range(st=parts[0], ed=parts[1], mi=mi, mx=mx, prefix=prefix)

            elif re.match(r"\d{1,2}/\d{1,2}$", expr):
                parts = expr.split("/")
                self.check_range(expr=parts[0], mi=mi, mx=mx, prefix=prefix)
                self.check_range(type_="interval", expr=parts[1], mi=mi, mx=mx, prefix=prefix)

            elif re.match(r"\d{1,2}-\d{1,2}/\d{1,2}$", expr):
                parts = expr.split("/")
                fst_parts = parts[0].split("-")
                self.check_range(expr=fst_parts[0], mi=mi, mx=mx, prefix=prefix)
                self.check_range(expr=fst_parts[1], mi=mi, mx=mx, prefix=prefix)
                self.compare_range(st=fst_parts[0], ed=fst_parts[1], mi=mi, mx=mx, prefix=prefix)
                self.check_range(type_="interval", expr=parts[1], mi=mi, mx=mx, prefix=prefix)

            elif re.match(r"\*/\d{1,2}$", expr):
                parts = expr.split("/")
                self.check_range(type_="interval", expr=parts[1], mi=mi, mx=mx, prefix=prefix)

            elif re.match(r"^(\d{1,2}|\d{1,2}-\d{1,2})(,\d{1,2}|,\d{1,2}-\d{1,2})+$", expr):
                limit = 60
                expr_ls = expr.split(",")
                if len(expr_ls) > limit:
                    msg = f"({prefix}) Exceeded maximum number({limit}) of specified value. '{len(expr_ls)}' is provided"
                    raise FormatError(msg)
                for n in expr_ls:
                    if "-" in n:
                        parts = n.split("-")
                        self.check_range(expr=parts[0], mi=mi, mx=mx, prefix=prefix)
                        self.check_range(expr=parts[1], mi=mi, mx=mx, prefix=prefix)
                        self.compare_range(st=parts[0], ed=parts[1], mi=mi, mx=mx, prefix=prefix)
                    else:
                        self.check_range(expr=n, mi=mi, mx=mx, prefix=prefix)
            else:
                msg = f"({prefix}) Illegal Expression Format '{expr}'"
                raise FormatError(msg)

        else:
            msg = f"({prefix}) Illegal Expression Format '{expr}'"
            raise FormatError(msg)

    def hour(self, expr: str, prefix: str) -> None:
        """ hour expressions (n : Number, s: String)
        *
        nn (1~23)
        nn-nn
        nn/nn
        nn-nn/nn
        */nn
        nn,nn,nn (Maximum 24 elements)
        """
        mi, mx = (0, 23)
        if re.match(r"\d{1,2}$", expr):
            self.check_range(expr=expr, mi=mi, mx=mx, prefix=prefix)

        elif re.search(r"[-*,/]", expr):
            if expr == "*":
                pass

            elif re.match(r"\d{1,2}-\d{1,2}$", expr):
                parts = expr.split("-")
                self.check_range(expr=parts[0], mi=mi, mx=mx, prefix=prefix)
                self.check_range(expr=parts[1], mi=mi, mx=mx, prefix=prefix)
                self.compare_range(st=parts[0], ed=parts[1], mi=mi, mx=mx, prefix=prefix)

            elif re.match(r"\d{1,2}/\d{1,2}$", expr):
                parts = expr.split("/")
                self.check_range(expr=parts[0], mi=mi, mx=mx, prefix=prefix)
                self.check_range(type_="interval", expr=parts[1], mi=mi, mx=mx, prefix=prefix)

            elif re.match(r"\d{1,2}-\d{1,2}/\d{1,2}$", expr):
                parts = expr.split("/")
                fst_parts = parts[0].split("-")
                self.check_range(expr=fst_parts[0], mi=mi, mx=mx, prefix=prefix)
                self.check_range(expr=fst_parts[1], mi=mi, mx=mx, prefix=prefix)
                self.compare_range(st=fst_parts[0], ed=fst_parts[1], mi=mi, mx=mx, prefix=prefix)
                self.check_range(type_="interval", expr=parts[1], mi=mi, mx=mx, prefix=prefix)

            elif re.match(r"\*/\d{1,2}$", expr):
                parts = expr.split("/")
                self.check_range(type_="interval", expr=parts[1], mi=mi, mx=mx, prefix=prefix)

            elif re.match(r"^(\d{1,2}|\d{1,2}-\d{1,2})(,\d{1,2}|,\d{1,2}-\d{1,2})+$", expr):
                limit = 24
                expr_ls = expr.split(",")
                if len(expr_ls) > limit:
                    msg = f"({prefix}) Exceeded maximum number({limit}) of specified value. '{len(expr_ls)}' is provided"
                    raise FormatError(msg)
                for n in expr_ls:
                    if "-" in n:
                        parts = n.split("-")
                        self.check_range(expr=parts[0], mi=mi, mx=mx, prefix=prefix)
                        self.check_range(expr=parts[1], mi=mi, mx=mx, prefix=prefix)
                        self.compare_range(st=parts[0], ed=parts[1], mi=mi, mx=mx, prefix=prefix)
                    else:
                        self.check_range(expr=n, mi=mi, mx=mx, prefix=prefix)
            else:
                msg = f"({prefix}) Illegal Expression Format '{expr}'"
                raise FormatError(msg)
        else:
            msg = f"({prefix}) Illegal Expression Format '{expr}'"
            raise FormatError(msg)

    def dayofmonth(self, expr: str, prefix: str) -> None:
        """ DAYOfMonth expressions (n : Number, s: String)
        *
        ?
        nn (1~31)
        nn-nn
        nn/nn
        nn-nn/nn
        */nn
        nn,nn,nn (Maximum 31 elements)
        L-nn
        LW
        nW
        """
        mi, mx = (1, 31)
        if re.match(r"\d{1,2}$", expr):
            self.check_range(expr=expr, mi=mi, mx=mx, prefix=prefix)
        elif re.search(r"[-*,/?]", expr):
            if expr in ("*", "?"):
                pass

            elif re.match(r"\d{1,2}-\d{1,2}$", expr):
                parts = expr.split("-")
                self.check_range(expr=parts[0], mi=mi, mx=mx, prefix=prefix)
                self.check_range(expr=parts[1], mi=mi, mx=mx, prefix=prefix)
                self.compare_range(st=parts[0], ed=parts[1], mi=mi, mx=mx, prefix=prefix)

            elif re.match(r"\d{1,2}/\d{1,2}$", expr):
                parts = expr.split("/")
                self.check_range(expr=parts[0], mi=mi, mx=mx, prefix=prefix)
                self.check_range(type_="interval", expr=parts[1], mi=0, mx=mx, prefix=prefix)

            elif re.match(r"\d{1,2}-\d{1,2}/\d{1,2}$", expr):
                parts = expr.split("/")
                fst_parts = parts[0].split("-")
                self.check_range(expr=fst_parts[0], mi=mi, mx=mx, prefix=prefix)
                self.check_range(expr=fst_parts[1], mi=mi, mx=mx, prefix=prefix)
                self.compare_range(st=fst_parts[0], ed=fst_parts[1], mi=mi, mx=mx, prefix=prefix)
                self.check_range(type_="interval", expr=parts[1], mi=0, mx=mx, prefix=prefix)

            elif re.match(r"\*/\d{1,2}$", expr):
                parts = expr.split("/")
                self.check_range(type_="interval", expr=parts[1], mi=0, mx=mx, prefix=prefix)

            elif re.match(r"^\d{1,2}(,\d{1,2})+$", expr):
                limit = 31
                expr_ls = expr.split(",")
                if len(expr_ls) > 31:
                    msg = f"({prefix}) Exceeded maximum number({limit}) of specified value. '{len(expr_ls)}' is provided"
                    raise FormatError(msg)
                for dayofmonth in expr_ls:
                    self.check_range(expr=dayofmonth, mi=mi, mx=mx, prefix=prefix)
            elif re.match(r"^([Ll])-(\d{1,2})$", expr):
                parts = expr.split("-")
                self.check_range(expr=parts[1], mi=mi, mx=mx, prefix=prefix)
            else:
                msg = f"Illegal Expression Format '{expr}'"
                raise FormatError(msg)

        elif re.match(r"^([Ll])([Ww])?$", expr) or re.match(r"^([Ww])([Ll])?$", expr):
            pass

        elif re.match(r"^(\d{1,2})([wW])$", expr):
            self.check_range(expr=expr[:-1], mi=mi, mx=mx, prefix=prefix)

        elif re.match(r"^([wW])(\d{1,2})$", expr):
            self.check_range(expr=expr[1:], mi=mi, mx=mx, prefix=prefix)

        else:
            msg = f"({prefix}) Illegal Expression Format '{expr}'"
            raise FormatError(msg)

    def month(self, expr: str, prefix: str) -> None:
        """ month expressions (n : Number, s: String)
        *
        nn (1~12)
        sss (JAN~DEC)
        nn-nn
        sss-sss
        nn/nn
        nn-nn/nn
        */nn
        nn,nn,nn,nn-nn,sss-sss (Maximum 12 elements)
        """
        mi, mx = (1, 12)
        if re.match(r"\d{1,2}$", expr):
            self.check_range(expr=expr, mi=mi, mx=mx, prefix=prefix)

        elif re.match(r"\D{3}$", expr):
            matched_month = [m for m in self._cron_months.values() if expr == m]
            if len(matched_month) == 0:
                msg = f"Invalid Month value '{expr}'"
                raise FormatError(msg)

        elif re.search(r"[-*,/]", expr):
            if expr == "*":
                pass

            elif re.match(r"\d{1,2}-\d{1,2}$", expr):
                parts = expr.split("-")
                self.check_range(expr=parts[0], mi=mi, mx=mx, prefix=prefix)
                self.check_range(expr=parts[1], mi=mi, mx=mx, prefix=prefix)
                self.compare_range(st=parts[0], ed=parts[1], mi=mi, mx=mx, prefix=prefix)

            elif re.match(r"\D{3}-\D{3}$", expr):
                parts = expr.split("-")
                cron_months = {v: k for (k, v) in self._cron_months.items()}
                st_not_exist = parts[0] not in cron_months
                ed_not_exist = parts[1] not in cron_months
                if st_not_exist or ed_not_exist:
                    msg = f"Invalid Month value '{expr}'"
                    raise FormatError(msg)
                self.compare_range(st=cron_months[parts[0]], ed=cron_months[parts[1]], mi=mi, mx=mx, prefix=prefix)

            elif re.match(r"\d{1,2}/\d{1,2}$", expr):
                parts = expr.split("/")
                self.check_range(expr=parts[0], mi=mi, mx=mx, prefix=prefix)
                self.check_range(type_="interval", expr=parts[1], mi=0, mx=mx, prefix=prefix)

            elif re.match(r"\d{1,2}-\d{1,2}/\d{1,2}$", expr):
                parts = expr.split("/")
                fst_parts = parts[0].split("-")
                self.check_range(expr=fst_parts[0], mi=mi, mx=mx, prefix=prefix)
                self.check_range(expr=fst_parts[1], mi=mi, mx=mx, prefix=prefix)
                self.compare_range(st=fst_parts[0], ed=fst_parts[1], mi=mi, mx=mx, prefix=prefix)
                self.check_range(type_="interval", expr=parts[1], mi=0, mx=12, prefix=prefix)

            elif re.match(r"\*/\d{1,2}$", expr):
                parts = expr.split("/")
                self.check_range(type_="interval", expr=parts[1], mi=0, mx=12, prefix=prefix)

            elif re.match(r"^\d{1,2}(,\d{1,2})+$", expr):
                limit = 12
                expr_ls = expr.split(",")
                if len(expr_ls) > limit:
                    msg = f"({prefix}) Exceeded maximum number({limit}) of specified value. '{len(expr_ls)}' is provided"
                    raise FormatError(msg)
                for month in expr_ls:
                    self.check_range(expr=month, mi=mi, mx=mx, prefix=prefix)

            elif re.match(r"^((\d{1,2}|\D{3})|(\D{3}-\D{3})|(\d{1,2}-\d{1,2}))((,\d{1,2})+"
                          r"|(,\D{3})*|(,\d{1,2}-\d{1,2})*|(,\D{3}-\D{3})*)*$", expr):
                """
                    1st Capture group : digit{1~2}|nondigit{3}|nondigit{3}-nondigit{3}|digit{3}-digit{3}
                    2nd Capture group : same with 1st capture group but repeated.
                """
                limit = 12
                expr_ls = expr.split(",")
                if len(expr_ls) > limit:
                    msg = f"({prefix}) Exceeded maximum number({limit}) of specified value. '{len(expr_ls)}' is provided"
                    raise FormatError(msg)
                cron_months = {v: k for (k, v) in self._cron_months.items()}
                for month in expr_ls:
                    if "-" in month:
                        parts = month.split("-")
                        if len(parts[0]) == 3:
                            self.check_range(expr=cron_months[parts[0].upper()], mi=mi, mx=mx, prefix=prefix)
                            self.check_range(expr=cron_months[parts[1].upper()], mi=mi, mx=mx, prefix=prefix)
                        else:
                            self.check_range(expr=parts[0], mi=mi, mx=mx, prefix=prefix)
                            self.check_range(expr=parts[1], mi=mi, mx=mx, prefix=prefix)
                            self.compare_range(st=parts[0], ed=parts[1], mi=mi, mx=mx, prefix=prefix)
                    else:
                        cron_month = cron_months[month.upper()] if len(month) == 3 else month
                        self.check_range(expr=cron_month, mi=mi, mx=mx, prefix=prefix)
            else:
                msg = f"({prefix}) Illegal Expression Format '{expr}'"
                raise FormatError(msg)
        else:
            msg = f"({prefix}) Illegal Expression Format '{expr}'"
            raise FormatError(msg)

    def dayofweek(self, expr: str, prefix: str) -> None:
        """ DAYOfWeek expressions (n : Number, s: String)
        *
        ?
        n (0~7) - 0 and 7 used interchangeable as Sunday
        sss (SUN~SAT)
        n/n
        n-n/n
        */n
        n-n
        sss-sss
        n|sss,n|sss,n|sss,n-n,sss-sss (maximum 7 elements)
        nL
        n#n
        """
        mi, mx = (0, 7)

        if expr in ("*", "?"):
            pass

        elif re.match(r"\d{1}$", expr):
            self.check_range(expr=expr, mi=mi, mx=mx, prefix=prefix)

        elif re.match(r"\D{3}$", expr):
            cron_days = {v: k for (k, v) in self._cron_days.items()}
            if expr.upper() in cron_days:
                pass
            else:
                msg = f"Invalid value '{expr}'"
                raise FormatError(msg)

        elif re.match(r"\d{1}/\d{1}$", expr):
            parts = expr.split("/")
            self.check_range(expr=parts[0], mi=mi, mx=mx, prefix=prefix)
            self.check_range(type_="interval", expr=parts[1], mi=0, mx=mx, prefix=prefix)

        elif re.match(r"\d{1}-\d{1}/\d{1}$", expr):
            parts = expr.split("/")
            fst_parts = parts[0].split("-")
            self.check_range(expr=fst_parts[0], mi=mi, mx=mx, prefix=prefix)
            self.check_range(expr=fst_parts[1], mi=mi, mx=mx, prefix=prefix)
            self.compare_range(st=fst_parts[0], ed=fst_parts[1], mi=mi, mx=mx, prefix=prefix)
            self.check_range(type_="interval", expr=parts[1], mi=0, mx=mx, prefix=prefix)

        elif re.match(r"[*]/\d{1}$", expr):
            parts = expr.split("/")
            self.check_range(type_="interval", expr=parts[1], mi=0, mx=mx, prefix=prefix)

        elif re.match(r"\d{1}-\d{1}$", expr):
            parts = expr.split("-")
            self.check_range(expr=parts[0], mi=mi, mx=mx, prefix=prefix)
            self.check_range(expr=parts[1], mi=mi, mx=mx, prefix=prefix)
            self.compare_range(st=parts[0], ed=parts[1], mi=mi, mx=mx, prefix=prefix)

        elif re.match(r"\D{3}-\D{3}$", expr):
            parts = expr.split("-")
            cron_days = {v: k for (k, v) in self._cron_days.items()}
            try:
                st_day = cron_days[parts[0].upper()]
                ed_day = cron_days[parts[1].upper()]
            except KeyError as e:
                msg = f"({prefix}) Invalid value '{expr}'"
                raise FormatError(msg) from e
            self.compare_range(st=st_day, ed=ed_day, mi=mi, mx=mx, prefix=prefix, type_="dow")

        elif re.match(r"^((\d{1}|\D{3})|(\D{3}-\D{3})|(\d{1}-\d{1}))"
                      r"((,\d{1})+|(,\D{3})*|(,\d{1}-\d{1})*|(,\D{3}-\D{3})*)*$", expr):
            limit = 7
            expr_ls = expr.split(",")
            if len(expr_ls) > limit:
                msg = f"({prefix}) Exceeded maximum number({limit}) of specified value. '{len(expr_ls)}' is provided"
                raise FormatError(msg)
            cron_days = {v: k for (k, v) in self._cron_days.items()}
            for day in expr_ls:
                if "-" in day:
                    parts = day.split("-")
                    if len(parts[0]) == 3:
                        self.check_range(expr=cron_days[parts[0].upper()], mi=mi, mx=mx, prefix=prefix)
                        self.check_range(expr=cron_days[parts[1].upper()], mi=mi, mx=mx, prefix=prefix)
                    else:
                        self.check_range(expr=parts[0], mi=mi, mx=mx, prefix=prefix)
                        self.check_range(expr=parts[1], mi=mi, mx=mx, prefix=prefix)
                        self.compare_range(st=parts[0], ed=parts[1], mi=mi, mx=mx, prefix=prefix)
                else:
                    # syncronize by add 1 to cron_days index
                    cron_day = cron_days[day.upper()] + 1 if len(day) == 3 else day
                    self.check_range(expr=cron_day, mi=mi, mx=mx, prefix=prefix)

        elif re.match(r"\d{1}([lL])$", expr):
            parts = expr.upper().split("L")
            self.check_range(expr=parts[0], mi=mi, mx=mx, prefix=prefix)

        elif re.match(r"\d#\d$", expr):
            parts = expr.split("#")
            self.check_range(expr=parts[0], mi=mi, mx=mx, prefix=prefix)
            self.check_range(expr=parts[1], mi=mi, mx=5, prefix=prefix, type_="dow")
        elif re.match(r"\D{3}#\d$", expr):
            parts = expr.split("#")
            cron_days = {v: k for (k, v) in self._cron_days.items()}
            try:
                st_day = cron_days[parts[0].upper()]
            except KeyError as e:
                msg = f"({prefix}) Invalid value '{expr}'"
                raise FormatError(msg) from e
            self.check_range(expr=parts[1], mi=mi, mx=5, prefix=prefix, type_="dow")
        else:
            msg = f"({prefix}) Illegal Expression Format '{expr}'"
            raise FormatError(msg)

    def year(self, expr: str, prefix: str) -> None:
        """ Year - valid expression (n : Number)
        *
        nnnn(1970~2099) - 4 digits number
        nnnn-nnnn(1970~2099)
        nnnn/nnn(0~129)
        */nnn(0~129)
        nnnn,nnnn,nnnn(1970~2099) - maximum 86 elements
        """
        mi, mx = (1970, 2099)
        if re.match(r"\d{4}$", expr):
            self.check_range(expr=expr, mi=mi, mx=mx, prefix=prefix)

        elif re.search(r"[-*,/]", expr):

            if expr == "*":
                pass

            elif re.match(r"\d{4}-\d{4}$", expr):
                parts = expr.split("-")
                self.check_range(expr=parts[0], mi=mi, mx=mx, prefix=prefix)
                self.check_range(expr=parts[1], mi=mi, mx=mx, prefix=prefix)
                self.compare_range(st=parts[0], ed=parts[1], mi=mi, mx=mx, prefix=prefix)

            elif re.match(r"\d{4}/\d{1,3}$", expr):
                parts = expr.split("/")
                self.check_range(expr=parts[0], mi=mi, mx=mx, prefix=prefix)
                self.check_range(type_="interval", expr=parts[1], mi=0, mx=129, prefix=prefix)

            elif re.match(r"\d{4}-\d{4}/\d{1,3}$", expr):
                parts = expr.split("/")
                fst_parts = parts[0].split("-")
                self.check_range(expr=fst_parts[0], mi=mi, mx=mx, prefix=prefix)
                self.check_range(expr=fst_parts[1], mi=mi, mx=mx, prefix=prefix)
                self.compare_range(st=fst_parts[0], ed=fst_parts[1], mi=mi, mx=mx, prefix=prefix)
                self.check_range(type_="interval", expr=parts[1], mi=0, mx=129, prefix=prefix)

            elif re.match(r"\*/\d{1,3}$", expr):
                parts = expr.split("/")
                self.check_range(type_="interval", expr=parts[1], mi=0, mx=129, prefix=prefix)

            elif re.match(r"\d{1}/\d{1,3}$", expr):
                parts = expr.split("/")
                self.check_range(expr=parts[0], mi=0, mx=129, prefix=prefix)
                self.check_range(type_="interval", expr=parts[1], mi=0, mx=129, prefix=prefix)

            elif re.match(r"^(\d{4}|\d{4}-\d{4})(,\d{4}|,\d{4}-\d{4})+$", expr):
                limit = 84
                expr_ls = expr.split(",")
                if len(expr_ls) > limit:
                    msg = f"({prefix}) Exceeded maximum number({limit}) of specified value. '{len(expr_ls)}' is provided"
                    raise FormatError(msg)
                for year in expr_ls:
                    if "-" in year:
                        parts = year.split("-")
                        self.check_range(expr=parts[0], mi=mi, mx=mx, prefix=prefix)
                        self.check_range(expr=parts[1], mi=mi, mx=mx, prefix=prefix)
                        self.compare_range(st=parts[0], ed=parts[1], mi=mi, mx=mx, prefix=prefix)
                    else:
                        self.check_range(expr=year, mi=mi, mx=mx, prefix=prefix)
            else:
                msg = f"({prefix}) Illegal Expression Format '{expr}'"
                raise FormatError(msg)
        else:
            msg = f"({prefix}) Illegal Expression Format '{expr}'"
            raise FormatError(msg)

    def check_range(self, prefix: str, mi: int, mx: int, expr: str | int, type_: str| None=None) -> None:
        """
        check if expression value within range of specified limit
        """
        if int(expr) < mi or mx < int(expr):
            if type_ is None:
                msg = f"{prefix} values must be between {mi} and {mx} but '{expr}' is provided"
            elif type_ == "interval":
                msg = f"({prefix}) Accepted increment value range is {mi}~{mx} but '{expr}' is provided"
            elif type_ == "dow":
                msg = f"({prefix}) Accepted week value is {mi}~{mx} but '{expr}' is provided"
            else:
                msg  = ""
            raise FormatError(msg)

    def compare_range(self, prefix: str, st: str | int, ed: str | int, mi:int, mx: int, type_: str | None=None) -> None:
        """ check 2 expression values size
        does not allow {st} value to be greater than {ed} value
        """
        st_int = int(st)
        ed_int = int(ed)
        if st_int > ed_int:
            if type_ is None:
                msg = f"({prefix}) Invalid range '{st}-{ed}'. Accepted range is {mi}-{mx}"
            elif type_ == "dow":
                msg = f"({prefix}) Invalid range '{self._cron_days[st_int]}-{self._cron_days[ed_int]}'. Accepted range is {mi}-{mx}"
            else:
                msg = ""
            raise FormatError(msg)
