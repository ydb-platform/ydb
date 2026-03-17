#!/usr/bin/env python
# (C) Copyright 2021 ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.
#

import datetime
import itertools
import json
import logging
import os
from collections import defaultdict

import requests
from multiurl import download, robust

from .date import (
    canonical_time,
    end_step,
    expand_date,
    expand_list,
    expand_time,
    full_date,
)
from .urls import URLS

LOG = logging.getLogger(__name__)

HOURLY_PATTERN = (
    "{_url}/{_yyyymmdd}/{_H}z/{model}/{resol}/{_stream}/"
    "{_yyyymmddHHMMSS}-{step}h-{_stream}-{type}.{_extension}"
)

MONTHLY_PATTERN = (
    "{_url}/{_yyyymmdd}/{_H}z/{model}/{resol}/{_stream}/"
    "{_yyyymmddHHMMSS}-{fcmonth}m-{_stream}-{type}.{_extension}"
)

PATTERNS = {"mmsa": MONTHLY_PATTERN}
EXTENSIONS = {"tf": "bufr"}

ONCE = set()

_ATTRIBUTION_SHOWN = False  # module-level guard to avoid spamming


def _show_attribution_message():
    global _ATTRIBUTION_SHOWN
    if not _ATTRIBUTION_SHOWN:
        print(
            "By downloading data from the ECMWF open data dataset, you agree to "
            "the terms: Attribution 4.0 International (CC BY 4.0). Please "
            "attribute ECMWF when downloading this data."
        )
        _ATTRIBUTION_SHOWN = True


def warning_once(*args, did_you_mean=None):
    if repr(args) in ONCE:
        return

    LOG.warning(*args)

    ONCE.add(repr(args))

    if did_you_mean:
        (words, vocabulary) = did_you_mean

        def levenshtein(a, b):
            if len(a) == 0:
                return len(b)

            if len(b) == 0:
                return len(a)

            if a[0].lower() == b[0].lower():
                return levenshtein(a[1:], b[1:])

            return 1 + min(
                [
                    levenshtein(a[1:], b[1:]),
                    levenshtein(a[1:], b),
                    levenshtein(a, b[1:]),
                ]
            )

        if not isinstance(words, (list, tuple)):
            words = [words]

        for word in words:
            distance, best = min((levenshtein(word, w), w) for w in vocabulary)
            if distance < min(len(word), len(best)):
                LOG.warning(
                    "Did you mean %r instead of %r?",
                    best,
                    word,
                )


class Result:
    def __init__(self, urls, target, dates, for_urls, for_index):
        self.urls = urls
        self.target = target
        if len(dates) == 1:
            self.datetime = dates[0]
        else:
            self.datetime = dates
        self.for_urls = for_urls
        self.for_index = for_index


class Client:
    def __init__(
        self,
        source="ecmwf",
        model="ifs",
        resol="0p25",
        beta=False,  # to access experimental data
        preserve_request_order=False,
        infer_stream_keyword=True,
        debug=False,
        verify=True,
        use_sas_token=None,
        sas_known_key="ecmwf",
        sas_custom_url=None,
    ):
        self._url = None
        self.source = source
        self.model = model
        self.resol = resol
        self.beta = beta
        self.preserve_request_order = preserve_request_order
        self.infer_stream_keyword = infer_stream_keyword
        self.session = requests.Session()
        self.verify = verify

        if source == "ecmwf":
            warning_once(
                "To ensure the stability of our systems and to "
                "preserve resources for our operational activities (network, compute, etc.), "
                "access to the open-data portal is limited to 500 simultaneous connections. "
                "This limit helps us guarantee reliable service for our operational users, "
                "especially during periods of high demand. "
                "For added reliability, the open-data is replicated across AWS, Azure, and Google Cloud."
                " If you experience difficulties accessing the portal directly, "
                "you can also retrieve the data from these cloud platforms."
            )

        # If the data source is azure, enable SAS token generation
        if use_sas_token is None:
            self.use_sas_token = source == "azure"
        else:
            self.use_sas_token = use_sas_token

        self.sas_token = None

        if debug:
            logging.basicConfig(level=logging.DEBUG)

        if self.use_sas_token:
            self.sas_token = self._get_azure_sas_token(
                known_key=sas_known_key,
                custom_url=sas_custom_url,
            )
            self.original_get = self.session.get
            self.original_head = self.session.head
            self.session.get = self._get_with_sas
            self.session.head = self._head_with_sas

    @property
    def url(self):
        if self._url is None:
            if self.source.startswith("http://") or self.source.startswith("https://"):
                self._url = self.source
            else:
                if self.source not in URLS:
                    warning_once(
                        "Unknown source %r. Known sources are %r",
                        self.source,
                        list(URLS.keys()),
                        did_you_mean=(self.source, list(URLS.keys())),
                    )
                    raise ValueError("Unknown source %r" % (self.source,))
                self._url = URLS[self.source]

        return self._url

    def retrieve(self, request=None, target=None, **kwargs):
        result = self._get_urls(request, target=target, use_index=True, **kwargs)

        if self.use_sas_token:
            result.urls = self._apply_sas_to_urls(result.urls)

        result.size = download(
            result.urls,
            target=result.target,
            verify=self.verify,
            session=self.session,
        )
        _show_attribution_message()
        return result

    def download(self, request=None, target=None, **kwargs):
        result = self._get_urls(request, target=target, use_index=False, **kwargs)

        if self.use_sas_token:
            result.urls = self._apply_sas_to_urls(result.urls)

        result.size = download(
            result.urls,
            target=result.target,
            verify=self.verify,
            session=self.session,
        )
        _show_attribution_message()
        return result

    def latest(self, request=None, **kwargs):
        if request is None:
            params = dict(**kwargs)
        else:
            params = dict(**request)

        if "time" not in params:
            delta = datetime.timedelta(hours=6)
        else:
            delta = datetime.timedelta(days=1)

        date = full_date(0, params.get("time", 18))

        stop = date - datetime.timedelta(days=2)

        while date > stop:
            result = self._get_urls(
                request=None,
                use_index=False,
                date=date,
                **params,
            )
            codes = [
                robust(self.session.head)(url, verify=self.verify).status_code
                for url in result.urls
            ]
            if len(codes) > 0 and all(c == 200 for c in codes):
                return date
            date -= delta

        raise ValueError("Cannot establish latest date for %r" % (result.for_urls,))

    def _get_urls(self, request=None, use_index=None, target=None, **kwargs):
        assert use_index in (True, False)
        if request is None:
            params = dict(**kwargs)
        else:
            params = dict(**request)

        if "date" not in params:
            params["date"] = self.latest(params)

        if target is None:
            target = params.pop("target", None)

        for_urls, for_index = self.prepare_request(params)

        for_urls["_url"] = [self.url]

        seen = set()
        data_urls = []

        dates = set()

        for args in (
            dict(zip(for_urls.keys(), x)) for x in itertools.product(*for_urls.values())
        ):
            pattern = PATTERNS.get(args["stream"], HOURLY_PATTERN)
            date = full_date(args.pop("date", None), args.pop("time", None))
            dates.add(date)
            args["_yyyymmdd"] = date.strftime("%Y%m%d")
            args["_H"] = date.strftime("%H")
            args["_yyyymmddHHMMSS"] = date.strftime("%Y%m%d%H%M%S")
            args["_extension"] = EXTENSIONS.get(args["type"], "grib2")
            args["_stream"] = self.patch_stream(args)

            if self.beta:
                # test data is put in an /experimental subdir after resol
                # it is not part of a mars request, so we inject it here
                args["resol"] += "/experimental"

            url = pattern.format(**args)

            if self.resol == "0p4-beta":
                url = url.replace("/ifs/", "/")

            if url not in seen:
                data_urls.append(url)
                seen.add(url)

        if for_index and use_index:
            data_urls = self.get_parts(data_urls, for_index)

        return Result(
            urls=data_urls,
            target=target,
            dates=sorted(dates),
            for_urls=for_urls,
            for_index=for_index,
        )

    def get_parts(self, data_urls, for_index):
        count = len(for_index)
        result = []
        line = None

        possible_values = defaultdict(set)

        for url in data_urls:
            base, _ = os.path.splitext(url)
            index_url = f"{base}.index"
            r = robust(self.session.get)(index_url, verify=self.verify)
            r.raise_for_status()

            parts = []
            for line in r.iter_lines():
                line = json.loads(line)
                matches = []
                for i, (name, values) in enumerate(for_index.items()):
                    idx = line.get(name)
                    if idx is not None:
                        possible_values[name].add(idx)
                    if idx in values:
                        if self.preserve_request_order:
                            for j, v in enumerate(values):
                                if v == idx:
                                    matches.append((i, j))
                        else:
                            matches.append(line["_offset"])

                if len(matches) == count:
                    parts.append((tuple(matches), (line["_offset"], line["_length"])))

            if parts:
                result.append((url, tuple(p[1] for p in sorted(parts))))

        for name, values in for_index.items():
            diff = set(values).difference(possible_values[name])
            for d in diff:
                warning_once(
                    "No index entries for %s=%s",
                    name,
                    d,
                    did_you_mean=(d, possible_values[name]),
                )

        if not result:
            raise ValueError("Cannot find index entries matching %r" % (for_index,))

        return result

    def user_to_index(self, key, value, request, for_index):
        FOR_INDEX = {
            ("type", "ef"): ["cf", "pf"],
        }

        return FOR_INDEX.get((key, value), value)

    def user_to_url(self, key, value, request, for_urls, model):
        FOR_URL = {
            ("type", "cf"): "ef",
            ("type", "pf"): "ef",
            ("type", "em"): "ep",
            ("type", "es"): "ep",
            ("type", "fcmean"): "fc",
            ("stream", "mmsa"): "mmsf",
        }

        # If the model is aifs-ens, we need to map the type to pf/cf because aifs-ens does not currently use ef
        if model == "aifs-ens":
            FOR_URL[("type", "pf")] = "pf"
            FOR_URL[("type", "cf")] = "cf"

        if key == "step" and for_urls["type"] == ["ep"]:
            if end_step(value) <= 240:
                return "240"
            else:
                return "360"

        return FOR_URL.get((key, value), value)

    def prepare_request(self, request=None, **kwargs):
        if request is None:
            params = dict(**kwargs)
        else:
            params = dict(**request)

        # If model is in the retireve overwrite the client model
        # Warn user if client model does not match the model in retrieve
        if "model" in params:
            if self.model != params["model"]:
                warning_once(
                    "Model %r does not match the client model %r, using model %r from retrieve",
                    params["model"],
                    self.model,
                    params["model"],
                    did_you_mean=(params["model"], self.model),
                )
            model = params["model"]
        else:
            model = self.model

        if "class" in params:
            model = {"od": "ifs", "ai": "aifs-single", "aifs-ens": "aifs-ens"}[
                params["class"]
            ]

        # Default stream for aifs-ens is enfo as this model only has ensemble forecasts
        if model == "aifs-ens":
            params["stream"] = "enfo"

        DEFAULTS_FC = dict(
            model=model,
            resol=self.resol,
            type="fc",
            stream="oper",
            step=0,
            fcmonth=1,
        )

        DEFAULTS_EF = dict(
            model=model,
            resol=self.resol,
            type=["cf", "pf"],
            stream="enfo",
            step=0,
            fcmonth=1,
        )

        DEFAULTS = {
            "enfo": DEFAULTS_EF,
            "waef": DEFAULTS_EF,
        }

        URL_COMPONENTS = (
            "date",
            "time",
            "model",
            "resol",
            "stream",
            "type",  # Must be before 'step' in that list
            "step",
            "fcmonth",
        )

        INDEX_COMPONENTS = (
            "param",
            "type",
            "step",
            "fcmonth",
            "number",
            "levelist",
            "levtype",
        )

        CANONICAL = {
            "time": lambda x: str(canonical_time(x)),
            # "param": lambda x: str(x).lower(),
            # "type": lambda x: str(x).lower(),
            # "stream": lambda x: str(x).lower(),
        }

        EXPAND_LIST = {
            "date": expand_date,
            "time": expand_time,
        }

        OTHER_STEP = {"mmsa": "step"}

        POSPROCESSING = {
            "area",
            "grid",
            "rotation",
            "frame",
            "bitmap",
            "gaussian",
            "accuracy",
            "format",
        }

        POSSIBLE_VALUES = {
            "type": ["tf", "fc", "fcmean", "cf", "pf", "em", "ep", "es"],
            "stream": ["oper", "wave", "scda", "scwv", "enfo", "waef", "mmsa"],
        }

        defaults = DEFAULTS.get(params.get("stream"), DEFAULTS_FC)
        for key, value in defaults.items():
            params.setdefault(key, value)

        params.pop("target", None)
        params.pop("class", None)

        params.pop(OTHER_STEP.get(params["stream"], "fcmonth"), None)

        postproc = POSPROCESSING.intersection(set(params.keys()))
        if postproc:
            warning_once("MARS post-processing keywords %r not supported", postproc)

        for_urls = defaultdict(list)
        for_index = defaultdict(list)
        ignored = set()

        def sorter(kv):
            a = kv[0]
            if a in URL_COMPONENTS:
                return URL_COMPONENTS.index(a)
            if a in INDEX_COMPONENTS:
                return INDEX_COMPONENTS.index(a) + len(URL_COMPONENTS)
            return len(URL_COMPONENTS) + len(INDEX_COMPONENTS)

        for k, v in sorted(params.items(), key=sorter):
            if isinstance(v, str):
                v = v.split("/")

            if not isinstance(v, (list, tuple)):
                v = [v]

            v = EXPAND_LIST.get(k, expand_list)(v)

            # Return canonical forms
            v = [CANONICAL.get(k, str)(x) for x in v]

            if k.startswith("_"):
                continue

            if k in POSSIBLE_VALUES:
                possible_values = POSSIBLE_VALUES[k]
                for value in v:
                    if value not in possible_values:
                        warning_once(
                            "Unknown value %r for keyword %r",
                            value,
                            k,
                            did_you_mean=(value, possible_values),
                        )

            if k in INDEX_COMPONENTS:
                for values in [self.user_to_index(k, x, params, for_index) for x in v]:
                    if not isinstance(values, (list, tuple)):
                        values = [values]
                    for value in values:
                        if value not in for_index[k]:
                            for_index[k].append(value)

            if k in URL_COMPONENTS:
                for values in [
                    self.user_to_url(k, x, params, for_urls, model) for x in v
                ]:
                    if not isinstance(values, (list, tuple)):
                        values = [values]
                    for value in values:
                        if value not in for_urls[k]:
                            for_urls[k].append(value)

            if (
                k not in URL_COMPONENTS
                and k not in INDEX_COMPONENTS
                and k not in POSPROCESSING
            ):
                ignored.add(k)

        if ignored:
            warning_once(
                "The following keywords %r are ignored",
                ignored,
                did_you_mean=(list(ignored), URL_COMPONENTS + INDEX_COMPONENTS),
            )

        if params.get("type") == "tf":
            for_index.clear()

        return (dict(**for_urls), dict(**for_index))

    def patch_stream(self, args):
        URL_STREAM_MAPPING = {
            ("oper", "06"): "scda",
            ("oper", "18"): "scda",
            ("wave", "06"): "scwv",
            ("wave", "18"): "scwv",
            #
            ("oper", "ef"): "enfo",
            ("wave", "ef"): "waef",
            ("oper", "ep"): "enfo",
            ("wave", "ep"): "waef",
            ("scda", "ef"): "enfo",
            ("scwv", "ef"): "waef",
            ("scda", "ep"): "enfo",
            ("scwv", "ep"): "waef",
        }
        stream, time, type = args["stream"], args["_H"], args["type"]

        if not self.infer_stream_keyword or args["model"] == "aifs-single":
            return stream

        stream = URL_STREAM_MAPPING.get((stream, time), stream)
        stream = URL_STREAM_MAPPING.get((stream, type), stream)

        return stream

    def _get_azure_sas_token(self, known_key="ecmwf", custom_url=None):
        known_urls = {
            "ecmwf": "https://planetarycomputer.microsoft.com/api/sas/v1/token/ai4edataeuwest/ecmwf"
        }
        if known_key in known_urls:
            url = known_urls[known_key]
        elif custom_url:
            url = custom_url
        else:
            raise ValueError("No known URL or custom URL provided")

        response = requests.get(url)
        response.raise_for_status()
        return response.json()["token"]

    def _add_sas_to_url(self, url):
        if "sig=" in url:
            return url
        return f"{url}&{self.sas_token}" if "?" in url else f"{url}?{self.sas_token}"

    def _get_with_sas(self, url, **kwargs):
        return self.original_get(self._add_sas_to_url(url), **kwargs)

    def _head_with_sas(self, url, **kwargs):
        return self.original_head(self._add_sas_to_url(url), **kwargs)

    def _apply_sas_to_urls(self, urls):
        updated_urls = []
        for item in urls:
            if isinstance(item, tuple):
                url, byte_ranges = item
                updated_urls.append((self._add_sas_to_url(url), byte_ranges))
            else:
                updated_urls.append(self._add_sas_to_url(item))
        return updated_urls
