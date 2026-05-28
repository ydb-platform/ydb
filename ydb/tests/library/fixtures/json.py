import json as std_json
import random
import string


class JsonCorpusGenerator:
    _MAX_DEPTH = 3
    _MAX_ARRAY_LEN = 4
    _MAX_OBJECT_FIELDS = 4
    _MAX_INDIVIDUAL_FIELDS = 2

    _COMMON_FIELDS = ("common_type", "common_version")
    _PARTIAL_FIELDS = (
        "tenant",
        "region",
        "service",
        "bucket",
        "active",
        "priority",
        "score",
    )

    def random_json(self):
        value = self._generate_value(depth=0, force_container=False)
        return std_json.dumps(value, ensure_ascii=True, separators=(",", ":"))

    def _random_string(self, min_len=2, max_len=12):
        length = random.randint(min_len, max_len)
        alphabet = string.ascii_lowercase + string.digits + "_- "
        return "".join(random.choice(alphabet) for _ in range(length)).strip() or "x"

    def _random_literal(self):
        literal_type = random.choice(("null", "bool", "int", "float", "string"))
        if literal_type == "null":
            return None
        if literal_type == "bool":
            return random.choice((True, False))
        if literal_type == "int":
            return random.randint(-10_000, 10_000)
        if literal_type == "float":
            return round(random.uniform(-10_000.0, 10_000.0), 3)
        return self._random_string()

    def _generate_array(self, depth, shape):
        length = random.randint(0, self._MAX_ARRAY_LEN)

        if depth >= self._MAX_DEPTH:
            return [self._random_literal() for _ in range(length)]

        if shape == "array_literals":
            return [self._random_literal() for _ in range(length)]
        if shape == "array_objects":
            return [self._generate_object(depth + 1, "object_literals") for _ in range(length)]
        if shape == "array_arrays":
            return [self._generate_array(depth + 1, "array_literals") for _ in range(length)]

        return [self._generate_value(depth + 1, force_container=False) for _ in range(length)]

    def _generate_object(self, depth, shape):
        obj = {}
        for field in self._COMMON_FIELDS:
            if field == "common_type":
                obj[field] = random.choice(("base", "extended", "compact"))
            else:
                obj[field] = random.randint(1, 5)

        partial_probability = random.uniform(0.1, 0.7)
        for key in self._PARTIAL_FIELDS:
            if random.random() < partial_probability:
                obj[key] = self._random_literal()

        individual_count = random.randint(0, self._MAX_INDIVIDUAL_FIELDS)
        for _ in range(individual_count):
            key = f"private_{random.randint(1, 9999)}"
            obj[key] = self._random_literal()

        if depth >= self._MAX_DEPTH:
            return obj

        nested_field_count = random.randint(0, self._MAX_OBJECT_FIELDS)
        for _ in range(nested_field_count):
            key = f"f_{self._random_string(3, 8).replace(' ', '_')}"
            if key in obj:
                continue
            if shape == "object_literals":
                obj[key] = self._random_literal()
            elif shape == "object_with_arrays":
                obj[key] = self._generate_array(depth + 1, random.choice(("array_literals", "array_arrays")))
            elif shape == "object_objects":
                obj[key] = self._generate_object(depth + 1, "object_literals")
            else:
                obj[key] = self._generate_value(depth + 1, force_container=False)

        return obj

    def _generate_value(self, depth=0, force_container=False):
        if depth >= self._MAX_DEPTH:
            return self._random_literal()

        shapes = [
            "literal",
            "array_literals",
            "object_literals",
            "array_objects",
            "object_with_arrays",
            "object_objects",
            "array_arrays",
            "mixed",
        ]

        if force_container:
            shapes = [shape for shape in shapes if shape != "literal"]

        shape = random.choice(shapes)
        if shape == "literal":
            return self._random_literal()
        if shape in ("array_literals", "array_objects", "array_arrays"):
            return self._generate_array(depth, shape)
        if shape in ("object_literals", "object_with_arrays", "object_objects"):
            return self._generate_object(depth, shape)
        return random.choice(
            (
                self._generate_array(depth, random.choice(("array_objects", "array_arrays"))),
                self._generate_object(depth, random.choice(("object_with_arrays", "object_objects"))),
            )
        )


class JsonPredicateGenerator:
    _COMMON_TYPES = ("base", "extended", "compact")
    _COMMON_VERSIONS = tuple(range(1, 6))

    def __init__(self, column="`json`", mode="strict"):
        self._column = column
        self._mode = mode
        self._atoms = []
        self._and_pairs = []
        self._or_pairs = []
        self._build_pool()

    def _je(self, path, suffix=""):
        return f"JSON_EXISTS({self._column}, '{self._mode} {path}'){suffix}"

    def _jv(self, path, returning, comparison):
        return (
            f"JSON_VALUE({self._column}, '{self._mode} {path}' "
            f"RETURNING {returning}) {comparison}"
        )

    def _add(self, sql):
        self._atoms.append(sql)

    def _build_pool(self):
        self._build_json_exists()
        self._build_json_value()
        self._build_compound_pairs()

    def _build_json_exists(self):
        self._add(self._je("$"))
        self._add(self._je("$") + " = true")
        self._add(self._je("$.common_type"))
        self._add(self._je("$.common_version"))
        for field in JsonCorpusGenerator._PARTIAL_FIELDS:
            self._add(self._je(f"$.{field}"))

        self._add(self._je("$.*"))
        self._add(self._je('$ ? (@.common_type starts with "b")'))
        self._add(self._je('$ ? (@.common_type starts with "e")'))

        for value in self._COMMON_TYPES:
            self._add(self._je(f'$.common_type ? (@ == "{value}")'))
            self._add(self._je(f'$ ? (@.common_type == "{value}")'))

        for version in self._COMMON_VERSIONS:
            self._add(self._je(f"$.common_version ? (@ == {version})"))

        self._add(self._je("$.common_version ? (@ >= 1 && @ <= 5)"))
        self._add(self._je("$.common_version ? (@ >= 2 && @ <= 4)"))
        self._add(self._je("$.common_version ? (@ > 0 && @ < 6)"))
        self._add(self._je("$.common_version ? (@ != 0)"))

        self._add(self._je("$.common_type ? (@ != null)"))
        self._add(self._je("$ ? (@.common_type != null)"))
        self._add(self._je("$.* ? (@ != null)"))

        self._add(self._je("$.active ? (@ == true)"))
        self._add(self._je("$.active ? (@ == false)"))
        self._add(self._je("$.active ? (@ != null)"))
        self._add(self._je("$.priority ? (@ > 0)"))
        self._add(self._je("$.priority ? (@ >= -10000 && @ <= 10000)"))
        self._add(self._je("$.score ? (@ != null)"))
        self._add(self._je("$.tenant ? (@ != null)"))
        self._add(self._je("$.region ? (@ != null)"))
        self._add(self._je("$.service ? (@ != null)"))
        self._add(self._je("$.bucket ? (@ != null)"))

        self._add(
            self._je(
                '$ ? (@.common_type == "base" && @.common_version >= 1 && @.common_version <= 5)'
            )
        )
        self._add(
            self._je(
                '$ ? (@.common_type == "extended" || @.common_type == "compact")'
            )
        )
        self._add(
            self._je(
                '$.common_type ? (@ == "base" || @ == "extended")'
            )
        )

        self._add(self._je('$.* ? (@ == "base")'))
        self._add(self._je('$.* ? (@ != null)'))
        self._add(self._je('$ ? (@.* != null)'))

        self._add(self._je("$[*]"))
        self._add(self._je("$.*[*]"))
        self._add(self._je("$.*[0]"))
        self._add(self._je("$.*[last]"))
        self._add(self._je("$.*[0 to 1]"))
        self._add(self._je("$.*[0, 2]"))
        self._add(self._je("$.*[*] ? (@ != null)"))
        self._add(self._je("$.*[*] ? (@ == true)"))
        self._add(self._je("$.*[*] ? (@ == false)"))
        self._add(self._je("$.*[0] ? (@ != null)"))
        self._add(self._je("$.*[last] ? (@ != null)"))

        self._add(self._je("$.*.common_type"))
        self._add(self._je("$.*.common_version"))
        self._add(self._je('$.*.common_type ? (@ == "base")'))
        self._add(self._je("$.*[*].common_type"))
        self._add(self._je('$ ? (@.common_type == "base" && @.common_version == 3)'))

        self._add(self._je('$.common_version.type() ? (@ == "number")'))
        self._add(self._je('$.common_type.type() ? (@ == "string")'))
        self._add(self._je("$.common_version.ceiling() ? (@ >= 1)"))
        self._add(self._je("$.common_version.floor() ? (@ >= 1)"))
        self._add(self._je("$.common_version.abs() ? (@ >= 1)"))
        self._add(self._je("$.common_version.ceiling() ? (@ <= 5)"))
        self._add(
            self._je(
                '$.keyvalue() ? (@.name == "common_type" && @.value == "base")'
            )
        )
        self._add(
            self._je(
                '$.keyvalue() ? (@.name == "common_version" && @.value != null)'
            )
        )
        self._add(self._je('$.keyvalue() ? (@.name starts with "common_")'))
        self._add(self._je('$.*.type() ? (@ == "string")'))
        self._add(self._je('$.*.type() ? (@ == "number")'))
        self._add(self._je('$.*.type() ? (@ == "boolean")'))
        self._add(self._je("$.*.size() ? (@ >= 0)"))
        self._add(self._je("$.*[*].size() ? (@ >= 0)"))

        self._add(self._je("$ ? (@ == null)"))
        self._add(self._je("$ ? (@ != null)"))
        self._add(self._je("$ ? (@ == true)"))
        self._add(self._je("$ ? (@ == false)"))
        self._add(self._je("$ ? (@ > -10000 && @ < 10000)"))

    def _build_json_value(self):
        for value in self._COMMON_TYPES:
            self._add(self._jv("$.common_type", "Utf8", f'= "{value}"'))

        for version in self._COMMON_VERSIONS:
            self._add(self._jv("$.common_version", "Int64", f"= {version}"))
            self._add(self._jv("$.common_version", "Int64", f"!= {version + 1}"))

        self._add(self._jv("$.common_version", "Int64", ">= 1"))
        self._add(self._jv("$.common_version", "Int64", "<= 5"))
        self._add(self._jv("$.common_version", "Int64", "> 0"))
        self._add(self._jv("$.common_version", "Int64", "< 6"))
        self._add(self._jv("$.common_version", "Double", ">= 1.0"))
        self._add(self._jv("$.common_version", "Double", "<= 5.0"))

        self._add(self._jv("$.common_type", "Utf8", '!= ""'))
        self._add(
            self._jv(
                "$.common_type",
                "Utf8",
                '= JSON_VALUE(' + self._column + f", '{self._mode} $.common_type' RETURNING Utf8)",
            )
        )

        self._add(self._jv("$.active", "Bool", "= true"))
        self._add(self._jv("$.active", "Bool", "!= false"))
        self._add(self._jv("$.priority", "Int64", "> -10000"))
        self._add(self._jv("$.priority", "Int64", "< 10000"))
        self._add(self._jv("$.score", "Double", ">= -10000.0"))
        self._add(self._jv("$.score", "Double", "<= 10000.0"))

        self._add(self._jv("$.common_version.ceiling()", "Int64", ">= 1"))
        self._add(self._jv("$.common_version.floor()", "Int64", ">= 1"))
        self._add(self._jv("$.common_version.abs()", "Int64", ">= 1"))
        self._add(self._jv('$.common_type.type()', "Utf8", '= "string"'))
        self._add(self._jv('$.common_version.type()', "Utf8", '= "number"'))
        self._add(
            self._jv(
                '$.keyvalue() ? (@.name == "common_type").value',
                "Utf8",
                '= "base"',
            )
        )

        self._add(self._jv("$.*[0]", "Utf8", '!= ""'))
        self._add(self._jv("$.*[0]", "Int64", ">= -10000"))
        self._add(self._jv("$.*[last]", "Utf8", '!= ""'))
        self._add(self._jv("$.*[*]", "Bool", "= true"))
        self._add(self._jv("$.*[*]", "Bool", "!= false"))

        self._add(self._jv("$ ? (@ == null)", "Bool", "= true"))
        self._add(self._jv("$ ? (@ != null)", "Bool", "= true"))
        self._add(self._jv("$ ? (@ == true)", "Bool", "= true"))
        self._add(self._jv("$ ? (@ == false)", "Bool", "= true"))

    def _build_compound_pairs(self):
        selective = [
            self._je('$.common_type ? (@ == "base")'),
            self._je('$.common_type ? (@ == "extended")'),
            self._je("$.common_version ? (@ == 3)"),
            self._je("$.common_version ? (@ >= 2 && @ <= 4)"),
            self._jv("$.common_type", "Utf8", '= "base"'),
            self._jv("$.common_version", "Int64", "= 3"),
        ]
        medium = [
            self._je("$.tenant"),
            self._je("$.region"),
            self._je("$.active ? (@ == true)"),
            self._je("$.*[*]"),
            self._je('$.*.common_type ? (@ == "base")'),
            self._jv("$.active", "Bool", "= true"),
        ]
        wide = [
            self._je("$.common_type"),
            self._je("$.common_version ? (@ >= 1 && @ <= 5)"),
            self._je("$.*"),
            self._je("$ ? (@.common_type != null)"),
        ]

        for a in selective:
            for b in wide:
                if a != b:
                    self._and_pairs.append(f"({a}) AND ({b})")

        for a in selective[:3]:
            for b in medium:
                if a != b:
                    self._and_pairs.append(f"({a}) AND ({b})")

        for a, b in (
            ('$.common_type ? (@ == "base")', '$.common_type ? (@ == "extended")'),
            ("$.tenant", "$.region"),
            ("$.region", "$.service"),
            ("$.priority ? (@ > 0)", "$.score ? (@ != null)"),
        ):
            self._or_pairs.append(f"({self._je(a)}) OR ({self._je(b)})")

        self._or_pairs.append(
            f"({self._jv('$.common_type', 'Utf8', '= \"base\"')}) OR "
            f"({self._jv('$.common_type', 'Utf8', '= \"extended\"')})"
        )

    def random_predicate(self):
        roll = random.random()
        if roll < 0.55:
            return random.choice(self._atoms)
        if roll < 0.75 and self._and_pairs:
            return random.choice(self._and_pairs)
        if roll < 0.90 and self._or_pairs:
            return random.choice(self._or_pairs)
        a, b = random.sample(self._atoms, 2)
        if random.random() < 0.7:
            return f"({a}) AND ({b})"
        return f"({a}) OR ({b})"


_PREDICATE_GENERATOR = None


def get_random_json():
    return JsonCorpusGenerator().random_json()


def get_random_predicate(column="`json`", mode="strict"):
    global _PREDICATE_GENERATOR
    if _PREDICATE_GENERATOR is None or (
        _PREDICATE_GENERATOR._column != column or _PREDICATE_GENERATOR._mode != mode
    ):
        _PREDICATE_GENERATOR = JsonPredicateGenerator(column=column, mode=mode)
    return _PREDICATE_GENERATOR.random_predicate()
