#  Licensed to Elasticsearch B.V. under one or more contributor
#  license agreements. See the NOTICE file distributed with
#  this work for additional information regarding copyright
#  ownership. Elasticsearch B.V. licenses this file to you under
#  the Apache License, Version 2.0 (the "License"); you may
#  not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
# 	http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

from typing import Any, ClassVar, Dict, List, Optional, Union, cast

from . import async_connections, connections
from .utils import AsyncUsingType, AttrDict, DslBase, UsingType, merge

__all__ = ["tokenizer", "analyzer", "char_filter", "token_filter", "normalizer"]


class AnalysisBase:
    @classmethod
    def _type_shortcut(
        cls,
        name_or_instance: Union[str, "AnalysisBase"],
        type: Optional[str] = None,
        **kwargs: Any,
    ) -> DslBase:
        if isinstance(name_or_instance, cls):
            if type or kwargs:
                raise ValueError(f"{cls.__name__}() cannot accept parameters.")
            return name_or_instance  # type: ignore[return-value]

        if not (type or kwargs):
            return cls.get_dsl_class("builtin")(name_or_instance)  # type: ignore[no-any-return, attr-defined]

        return cls.get_dsl_class(type, "custom")(  # type: ignore[no-any-return, attr-defined]
            name_or_instance, type or "custom", **kwargs
        )


class CustomAnalysis:
    name = "custom"

    def __init__(self, filter_name: str, builtin_type: str = "custom", **kwargs: Any):
        self._builtin_type = builtin_type
        self._name = filter_name
        super().__init__(**kwargs)

    def to_dict(self) -> Dict[str, Any]:
        # only name to present in lists
        return self._name  # type: ignore[return-value]

    def get_definition(self) -> Dict[str, Any]:
        d = super().to_dict()  # type: ignore[misc]
        d = d.pop(self.name)
        d["type"] = self._builtin_type
        return d  # type: ignore[no-any-return]


class CustomAnalysisDefinition(CustomAnalysis):
    _type_name: str
    _param_defs: ClassVar[Dict[str, Any]]
    filter: List[Any]
    char_filter: List[Any]

    def get_analysis_definition(self) -> Dict[str, Any]:
        out = {self._type_name: {self._name: self.get_definition()}}

        t = cast("Tokenizer", getattr(self, "tokenizer", None))
        if "tokenizer" in self._param_defs and hasattr(t, "get_definition"):
            out["tokenizer"] = {t._name: t.get_definition()}

        filters = {
            f._name: f.get_definition()
            for f in self.filter
            if hasattr(f, "get_definition")
        }
        if filters:
            out["filter"] = filters

        # any sub filter definitions like multiplexers etc?
        for f in self.filter:
            if hasattr(f, "get_analysis_definition"):
                d = f.get_analysis_definition()
                if d:
                    merge(out, d, True)

        char_filters = {
            f._name: f.get_definition()
            for f in self.char_filter
            if hasattr(f, "get_definition")
        }
        if char_filters:
            out["char_filter"] = char_filters

        return out


class BuiltinAnalysis:
    name = "builtin"

    def __init__(self, name: str):
        self._name = name
        super().__init__()

    def to_dict(self) -> Dict[str, Any]:
        # only name to present in lists
        return self._name  # type: ignore[return-value]


class Analyzer(AnalysisBase, DslBase):
    _type_name = "analyzer"
    name = ""


class BuiltinAnalyzer(BuiltinAnalysis, Analyzer):
    def get_analysis_definition(self) -> Dict[str, Any]:
        return {}


class CustomAnalyzer(CustomAnalysisDefinition, Analyzer):
    _param_defs = {
        "filter": {"type": "token_filter", "multi": True},
        "char_filter": {"type": "char_filter", "multi": True},
        "tokenizer": {"type": "tokenizer"},
    }

    def _get_body(
        self, text: str, explain: bool, attributes: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        body = {"text": text, "explain": explain}
        if attributes:
            body["attributes"] = attributes

        definition = self.get_analysis_definition()
        analyzer_def = self.get_definition()

        for section in ("tokenizer", "char_filter", "filter"):
            if section not in analyzer_def:
                continue
            sec_def = definition.get(section, {})
            sec_names = analyzer_def[section]

            if isinstance(sec_names, str):
                body[section] = sec_def.get(sec_names, sec_names)
            else:
                body[section] = [
                    sec_def.get(sec_name, sec_name) for sec_name in sec_names
                ]

        if self._builtin_type != "custom":
            body["analyzer"] = self._builtin_type

        return body

    def simulate(
        self,
        text: str,
        using: UsingType = "default",
        explain: bool = False,
        attributes: Optional[Dict[str, Any]] = None,
    ) -> AttrDict[Any]:
        """
        Use the Analyze API of elasticsearch to test the outcome of this analyzer.

        :arg text: Text to be analyzed
        :arg using: connection alias to use, defaults to ``'default'``
        :arg explain: will output all token attributes for each token. You can
            filter token attributes you want to output by setting ``attributes``
            option.
        :arg attributes: if ``explain`` is specified, filter the token
            attributes to return.
        """
        es = connections.get_connection(using)
        return AttrDict(
            cast(
                Dict[str, Any],
                es.indices.analyze(body=self._get_body(text, explain, attributes)),
            )
        )

    async def async_simulate(
        self,
        text: str,
        using: AsyncUsingType = "default",
        explain: bool = False,
        attributes: Optional[Dict[str, Any]] = None,
    ) -> AttrDict[Any]:
        """
        Use the Analyze API of elasticsearch to test the outcome of this analyzer.

        :arg text: Text to be analyzed
        :arg using: connection alias to use, defaults to ``'default'``
        :arg explain: will output all token attributes for each token. You can
            filter token attributes you want to output by setting ``attributes``
            option.
        :arg attributes: if ``explain`` is specified, filter the token
            attributes to return.
        """
        es = async_connections.get_connection(using)
        return AttrDict(
            cast(
                Dict[str, Any],
                await es.indices.analyze(
                    body=self._get_body(text, explain, attributes)
                ),
            )
        )


class Normalizer(AnalysisBase, DslBase):
    _type_name = "normalizer"
    name = ""


class BuiltinNormalizer(BuiltinAnalysis, Normalizer):
    def get_analysis_definition(self) -> Dict[str, Any]:
        return {}


class CustomNormalizer(CustomAnalysisDefinition, Normalizer):
    _param_defs = {
        "filter": {"type": "token_filter", "multi": True},
        "char_filter": {"type": "char_filter", "multi": True},
    }


class Tokenizer(AnalysisBase, DslBase):
    _type_name = "tokenizer"
    name = ""


class BuiltinTokenizer(BuiltinAnalysis, Tokenizer):
    pass


class CustomTokenizer(CustomAnalysis, Tokenizer):
    pass


class TokenFilter(AnalysisBase, DslBase):
    _type_name = "token_filter"
    name = ""


class BuiltinTokenFilter(BuiltinAnalysis, TokenFilter):
    pass


class CustomTokenFilter(CustomAnalysis, TokenFilter):
    pass


class MultiplexerTokenFilter(CustomTokenFilter):
    name = "multiplexer"

    def get_definition(self) -> Dict[str, Any]:
        d = super(CustomTokenFilter, self).get_definition()

        if "filters" in d:
            d["filters"] = [
                # comma delimited string given by user
                (
                    fs
                    if isinstance(fs, str)
                    else
                    # list of strings or TokenFilter objects
                    ", ".join(f.to_dict() if hasattr(f, "to_dict") else f for f in fs)
                )
                for fs in self.filters
            ]
        return d

    def get_analysis_definition(self) -> Dict[str, Any]:
        if not hasattr(self, "filters"):
            return {}

        fs: Dict[str, Any] = {}
        d = {"filter": fs}
        for filters in self.filters:
            if isinstance(filters, str):
                continue
            fs.update(
                {
                    f._name: f.get_definition()
                    for f in filters
                    if hasattr(f, "get_definition")
                }
            )
        return d


class ConditionalTokenFilter(CustomTokenFilter):
    name = "condition"

    def get_definition(self) -> Dict[str, Any]:
        d = super(CustomTokenFilter, self).get_definition()
        if "filter" in d:
            d["filter"] = [
                f.to_dict() if hasattr(f, "to_dict") else f for f in self.filter
            ]
        return d

    def get_analysis_definition(self) -> Dict[str, Any]:
        if not hasattr(self, "filter"):
            return {}

        return {
            "filter": {
                f._name: f.get_definition()
                for f in self.filter
                if hasattr(f, "get_definition")
            }
        }


class CharFilter(AnalysisBase, DslBase):
    _type_name = "char_filter"
    name = ""


class BuiltinCharFilter(BuiltinAnalysis, CharFilter):
    pass


class CustomCharFilter(CustomAnalysis, CharFilter):
    pass


# shortcuts for direct use
analyzer = Analyzer._type_shortcut
tokenizer = Tokenizer._type_shortcut
token_filter = TokenFilter._type_shortcut
char_filter = CharFilter._type_shortcut
normalizer = Normalizer._type_shortcut
