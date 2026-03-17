# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
#
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.
#
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

from typing import Any, Optional

from opensearchpy.connection.connections import get_connection

from .utils import AttrDict, DslBase, merge


class AnalysisBase:
    @classmethod
    def _type_shortcut(
        cls: Any, name_or_instance: Any, type: Any = None, **kwargs: Any
    ) -> Any:
        if isinstance(name_or_instance, cls):
            if type or kwargs:
                raise ValueError(f"{cls.__name__}() cannot accept parameters.")
            return name_or_instance

        if not (type or kwargs):
            return cls.get_dsl_class("builtin")(name_or_instance)

        return cls.get_dsl_class(type, "custom")(
            name_or_instance, type or "custom", **kwargs
        )


class CustomAnalysis:
    name: Optional[str] = "custom"

    def __init__(
        self, filter_name: str, builtin_type: str = "custom", **kwargs: Any
    ) -> None:
        self._builtin_type = builtin_type
        self._name = filter_name
        super().__init__(**kwargs)

    def to_dict(self) -> Any:
        # only name to present in lists
        return self._name

    def get_definition(self) -> Any:
        d = super().to_dict()  # type: ignore
        d = d.pop(self.name)
        d["type"] = self._builtin_type
        return d


class CustomAnalysisDefinition(CustomAnalysis):
    def get_analysis_definition(self: Any) -> Any:
        out = {self._type_name: {self._name: self.get_definition()}}

        t: Any = getattr(self, "tokenizer", None)
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
    name: Optional[str] = "builtin"

    def __init__(self, name: Any) -> None:
        self._name = name
        super().__init__()

    def to_dict(self) -> Any:
        # only name to present in lists
        return self._name


class Analyzer(AnalysisBase, DslBase):
    _type_name: str = "analyzer"
    name: Optional[str] = None


class BuiltinAnalyzer(BuiltinAnalysis, Analyzer):
    def get_analysis_definition(self) -> Any:
        return {}


class CustomAnalyzer(CustomAnalysisDefinition, Analyzer):
    _param_defs = {
        "filter": {"type": "token_filter", "multi": True},
        "char_filter": {"type": "char_filter", "multi": True},
        "tokenizer": {"type": "tokenizer"},
    }

    def simulate(
        self,
        text: Any,
        using: str = "default",
        explain: bool = False,
        attributes: Any = None,
    ) -> Any:
        """
        Use the Analyze API of opensearch to test the outcome of this analyzer.

        :arg text: Text to be analyzed
        :arg using: connection alias to use, defaults to ``'default'``
        :arg explain: will output all token attributes for each token. You can
            filter token attributes you want to output by setting ``attributes``
            option.
        :arg attributes: if ``explain`` is specified, filter the token
            attributes to return.
        """
        opensearch = get_connection(using)

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

        return AttrDict(opensearch.indices.analyze(body=body))


class Normalizer(AnalysisBase, DslBase):
    _type_name: str = "normalizer"
    name: Optional[str] = None


class BuiltinNormalizer(BuiltinAnalysis, Normalizer):
    def get_analysis_definition(self) -> Any:
        return {}


class CustomNormalizer(CustomAnalysisDefinition, Normalizer):
    _param_defs = {
        "filter": {"type": "token_filter", "multi": True},
        "char_filter": {"type": "char_filter", "multi": True},
    }


class Tokenizer(AnalysisBase, DslBase):
    _type_name: str = "tokenizer"
    name: Optional[str] = None


class BuiltinTokenizer(BuiltinAnalysis, Tokenizer):
    pass


class CustomTokenizer(CustomAnalysis, Tokenizer):
    pass


class TokenFilter(AnalysisBase, DslBase):
    _type_name: str = "token_filter"
    name: Optional[str] = None


class BuiltinTokenFilter(BuiltinAnalysis, TokenFilter):
    pass


class CustomTokenFilter(CustomAnalysis, TokenFilter):
    pass


class MultiplexerTokenFilter(CustomTokenFilter):
    name = "multiplexer"

    def get_definition(self) -> Any:
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

    def get_analysis_definition(self) -> Any:
        if not hasattr(self, "filters"):
            return {}

        fs: Any = {}
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

    def get_definition(self) -> Any:
        d = super(CustomTokenFilter, self).get_definition()
        if "filter" in d:
            d["filter"] = [
                f.to_dict() if hasattr(f, "to_dict") else f for f in self.filter
            ]
        return d

    def get_analysis_definition(self) -> Any:
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
    _type_name: str = "char_filter"
    name: Optional[str] = None


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

__all__ = ["tokenizer", "analyzer", "char_filter", "token_filter", "normalizer"]
