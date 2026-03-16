"""
.. codeauthor:: Tsuyoshi Hombashi <tsuyoshi.hombashi@gmail.com>
"""

from collections import OrderedDict

from allpairspy import AllPairs


class Test_pairewise_OrderedDict:
    def test_normal(self):
        parameters = OrderedDict(
            {"brand": ["Brand X", "Brand Y"], "os": ["NT", "2000", "XP"], "minute": [15, 30, 60]}
        )

        for pairs in AllPairs(parameters):
            assert pairs.brand == "Brand X"
            assert pairs.os == "NT"
            assert pairs.minute == 15
            break

        assert len(list(AllPairs(parameters))) == 9


class Test_pairewise_list:
    # example1.1.py

    def test_normal(self):
        parameters = [
            ["Brand X", "Brand Y"],
            ["98", "NT", "2000", "XP"],
            ["Internal", "Modem"],
            ["Salaried", "Hourly", "Part-Time", "Contr."],
            [6, 10, 15, 30, 60],
        ]

        assert list(AllPairs(parameters)) == [
            ["Brand X", "98", "Internal", "Salaried", 6],
            ["Brand Y", "NT", "Modem", "Hourly", 6],
            ["Brand Y", "2000", "Internal", "Part-Time", 10],
            ["Brand X", "XP", "Modem", "Contr.", 10],
            ["Brand X", "2000", "Modem", "Part-Time", 15],
            ["Brand Y", "XP", "Internal", "Hourly", 15],
            ["Brand Y", "98", "Modem", "Salaried", 30],
            ["Brand X", "NT", "Internal", "Contr.", 30],
            ["Brand X", "98", "Internal", "Hourly", 60],
            ["Brand Y", "2000", "Modem", "Contr.", 60],
            ["Brand Y", "NT", "Modem", "Salaried", 60],
            ["Brand Y", "XP", "Modem", "Part-Time", 60],
            ["Brand Y", "2000", "Modem", "Hourly", 30],
            ["Brand Y", "98", "Modem", "Contr.", 15],
            ["Brand Y", "XP", "Modem", "Salaried", 15],
            ["Brand Y", "NT", "Modem", "Part-Time", 15],
            ["Brand Y", "XP", "Modem", "Part-Time", 30],
            ["Brand Y", "98", "Modem", "Part-Time", 6],
            ["Brand Y", "2000", "Modem", "Salaried", 6],
            ["Brand Y", "98", "Modem", "Salaried", 10],
            ["Brand Y", "XP", "Modem", "Contr.", 6],
            ["Brand Y", "NT", "Modem", "Hourly", 10],
        ]


class Test_triplewise:
    # example1.2.py

    def test_normal(self):
        parameters = [
            ["Brand X", "Brand Y"],
            ["98", "NT", "2000", "XP"],
            ["Internal", "Modem"],
            ["Salaried", "Hourly", "Part-Time", "Contr."],
            [6, 10, 15, 30, 60],
        ]

        assert list(AllPairs(parameters, n=3)) == [
            ["Brand X", "98", "Internal", "Salaried", 6],
            ["Brand Y", "NT", "Modem", "Hourly", 6],
            ["Brand Y", "2000", "Modem", "Part-Time", 10],
            ["Brand X", "XP", "Internal", "Contr.", 10],
            ["Brand X", "XP", "Modem", "Part-Time", 6],
            ["Brand Y", "2000", "Internal", "Hourly", 15],
            ["Brand Y", "NT", "Internal", "Salaried", 10],
            ["Brand X", "98", "Modem", "Contr.", 15],
            ["Brand X", "98", "Modem", "Hourly", 10],
            ["Brand Y", "NT", "Modem", "Contr.", 30],
            ["Brand X", "XP", "Internal", "Hourly", 30],
            ["Brand X", "2000", "Modem", "Salaried", 30],
            ["Brand Y", "2000", "Internal", "Contr.", 6],
            ["Brand Y", "NT", "Internal", "Part-Time", 60],
            ["Brand Y", "XP", "Modem", "Salaried", 15],
            ["Brand X", "98", "Modem", "Part-Time", 60],
            ["Brand X", "XP", "Modem", "Salaried", 60],
            ["Brand X", "2000", "Internal", "Part-Time", 15],
            ["Brand X", "2000", "Modem", "Contr.", 60],
            ["Brand X", "98", "Modem", "Salaried", 10],
            ["Brand X", "98", "Modem", "Part-Time", 30],
            ["Brand X", "NT", "Modem", "Part-Time", 10],
            ["Brand Y", "NT", "Modem", "Salaried", 60],
            ["Brand Y", "NT", "Modem", "Hourly", 15],
            ["Brand Y", "NT", "Modem", "Hourly", 30],
            ["Brand Y", "NT", "Modem", "Hourly", 60],
            ["Brand Y", "NT", "Modem", "Hourly", 10],
        ]


class Test_pairewise_w_tested:
    # example1.3.py

    def test_normal(self):
        parameters = [
            ["Brand X", "Brand Y"],
            ["98", "NT", "2000", "XP"],
            ["Internal", "Modem"],
            ["Salaried", "Hourly", "Part-Time", "Contr."],
            [6, 10, 15, 30, 60],
        ]
        tested = [
            ["Brand X", "98", "Modem", "Hourly", 10],
            ["Brand X", "98", "Modem", "Hourly", 15],
            ["Brand Y", "NT", "Internal", "Part-Time", 10],
        ]

        assert list(AllPairs(parameters, previously_tested=tested)) == [
            ["Brand Y", "2000", "Modem", "Salaried", 6],
            ["Brand X", "XP", "Internal", "Contr.", 6],
            ["Brand Y", "XP", "Modem", "Contr.", 30],
            ["Brand X", "2000", "Internal", "Part-Time", 30],
            ["Brand Y", "98", "Internal", "Salaried", 60],
            ["Brand X", "NT", "Modem", "Salaried", 60],
            ["Brand Y", "XP", "Internal", "Hourly", 15],
            ["Brand Y", "NT", "Modem", "Hourly", 30],
            ["Brand Y", "2000", "Modem", "Part-Time", 15],
            ["Brand Y", "2000", "Modem", "Contr.", 10],
            ["Brand Y", "XP", "Modem", "Salaried", 10],
            ["Brand Y", "98", "Modem", "Part-Time", 6],
            ["Brand Y", "NT", "Modem", "Contr.", 15],
            ["Brand Y", "98", "Modem", "Contr.", 30],
            ["Brand Y", "XP", "Modem", "Part-Time", 60],
            ["Brand Y", "2000", "Modem", "Hourly", 60],
            ["Brand Y", "NT", "Modem", "Salaried", 30],
            ["Brand Y", "NT", "Modem", "Salaried", 15],
            ["Brand Y", "NT", "Modem", "Hourly", 6],
            ["Brand Y", "NT", "Modem", "Contr.", 60],
        ]


class Test_pairewise_filter:
    def test_normal_example21(self):
        # example2.1.py

        parameters = [
            ["Brand X", "Brand Y"],
            ["98", "NT", "2000", "XP"],
            ["Internal", "Modem"],
            ["Salaried", "Hourly", "Part-Time", "Contr."],
            [6, 10, 15, 30, 60],
        ]

        def is_valid_combination(row):
            """
            Should return True if combination is valid and False otherwise.

            Test row that is passed here can be incomplete.
            To prevent search for unnecessary items filtering function
            is executed with found subset of data to validate it.
            """

            n = len(row)
            if n > 1:
                # Brand Y does not support Windows 98
                if "98" == row[1] and "Brand Y" == row[0]:
                    return False
                # Brand X does not work with XP
                if "XP" == row[1] and "Brand X" == row[0]:
                    return False
            if n > 4:
                # Contractors are billed in 30 min increments
                if "Contr." == row[3] and row[4] < 30:
                    return False

            return True

        assert list(AllPairs(parameters, filter_func=is_valid_combination)) == [
            ["Brand X", "98", "Internal", "Salaried", 6],
            ["Brand Y", "NT", "Modem", "Hourly", 6],
            ["Brand Y", "2000", "Internal", "Part-Time", 10],
            ["Brand X", "2000", "Modem", "Contr.", 30],
            ["Brand X", "NT", "Internal", "Contr.", 60],
            ["Brand Y", "XP", "Modem", "Salaried", 60],
            ["Brand X", "98", "Modem", "Part-Time", 15],
            ["Brand Y", "XP", "Internal", "Hourly", 15],
            ["Brand Y", "NT", "Internal", "Part-Time", 30],
            ["Brand X", "2000", "Modem", "Hourly", 10],
            ["Brand Y", "XP", "Modem", "Contr.", 30],
            ["Brand Y", "2000", "Modem", "Salaried", 15],
            ["Brand Y", "NT", "Modem", "Salaried", 10],
            ["Brand Y", "XP", "Modem", "Part-Time", 6],
            ["Brand Y", "2000", "Modem", "Contr.", 60],
        ]

    def test_normal_example22(self):
        # example2.2.py

        parameters = [
            ("brand", ["Brand X", "Brand Y"]),
            ("os", ["98", "NT", "2000", "XP"]),
            ("network", ["Internal", "Modem"]),
            ("employee", ["Salaried", "Hourly", "Part-Time", "Contr."]),
            ("increment", [6, 10, 15, 30, 60]),
        ]

        def is_valid_combination(values, names):
            dictionary = dict(zip(names, values))

            """
            Should return True if combination is valid and False otherwise.

            Dictionary that is passed here can be incomplete.
            To prevent search for unnecessary items filtering function
            is executed with found subset of data to validate it.
            """

            rules = [
                # Brand Y does not support Windows 98
                # Brand X does not work with XP
                # Contractors are billed in 30 min increments
                lambda d: "98" == d["os"] and "Brand Y" == d["brand"],
                lambda d: "XP" == d["os"] and "Brand X" == d["brand"],
                lambda d: "Contr." == d["employee"] and d["increment"] < 30,
            ]

            for rule in rules:
                try:
                    if rule(dictionary):
                        return False
                except KeyError:
                    pass

            return True

        assert list(
            AllPairs(
                [x[1] for x in parameters],
                filter_func=lambda values: is_valid_combination(values, [x[0] for x in parameters]),
            )
        ) == [
            ["Brand X", "98", "Internal", "Salaried", 6],
            ["Brand Y", "NT", "Modem", "Hourly", 6],
            ["Brand Y", "2000", "Internal", "Part-Time", 10],
            ["Brand X", "2000", "Modem", "Contr.", 30],
            ["Brand X", "NT", "Internal", "Contr.", 60],
            ["Brand Y", "XP", "Modem", "Salaried", 60],
            ["Brand X", "98", "Modem", "Part-Time", 15],
            ["Brand Y", "XP", "Internal", "Hourly", 15],
            ["Brand Y", "NT", "Internal", "Part-Time", 30],
            ["Brand X", "2000", "Modem", "Hourly", 10],
            ["Brand Y", "XP", "Modem", "Contr.", 30],
            ["Brand Y", "2000", "Modem", "Salaried", 15],
            ["Brand Y", "NT", "Modem", "Salaried", 10],
            ["Brand Y", "XP", "Modem", "Part-Time", 6],
            ["Brand Y", "2000", "Modem", "Contr.", 60],
        ]
