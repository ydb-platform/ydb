# -*- coding: utf-8 -*-
# Copyright (c) 2013-2014 Will Thames <will@thames.id.au>
# Modified work Copyright (c) 2020-2021 Warpnet B.V.

from collections import defaultdict
import os
import codecs
import sys

from saltlint.config import Configuration
from saltlint.utils import load_plugins


class RulesCollection(object):

    def __init__(self, config=Configuration()):
        self.rules = []
        self.config = config

    def register(self, obj):
        if not any(rule.id == obj.id for rule in self.rules):
            self.rules.append(obj)

    def __iter__(self):
        return iter(self.rules)

    def __len__(self):
        return len(self.rules)

    def extend(self, more):
        self.rules.extend(more)

    def run(self, statefile, tags=set(), skip_list=frozenset()):
        text = ""
        matches = []

        try:
            with codecs.open(statefile['path'], mode='rb', encoding='utf-8') as f:
                text = f.read()
        except IOError as e:
            print("WARNING: Couldn't open %s - %s" %
                  (statefile['path'], e.strerror),
                  file=sys.stderr)
            return matches

        for rule in self.rules:
            skip = False
            if not tags or not set(rule.tags).union([rule.id]).isdisjoint(tags):
                rule_definition = set(rule.tags)
                rule_definition.add(rule.id)

                # Check if the file is in the rule specific ignore list.
                for definition in rule_definition:
                    if self.config.is_file_ignored(statefile['path'], definition):
                        skip = True
                        break

                if not skip and set(rule_definition).isdisjoint(skip_list):
                    matches.extend(rule.matchlines(statefile, text))
                    matches.extend(rule.matchfulltext(statefile, text))

        return matches

    def __repr__(self):
        return "\n".join([rule.verbose()
                          for rule in sorted(self.rules, key=lambda x: x.id)])

    def listtags(self):
        tags = defaultdict(list)
        for rule in self.rules:
            for tag in rule.tags:
                tags[tag].append("[{0}]".format(rule.id))
        results = []
        for tag in sorted(tags):
            results.append("{0} {1}".format(tag, tags[tag]))
        return "\n".join(results)

    @classmethod
    def create_from_directory(cls, rulesdir, config):
        result = cls(config)
        result.rules = load_plugins(os.path.expanduser(rulesdir), config)
        return result
