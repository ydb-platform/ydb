# -*- coding: utf-8 -*-
import regex as re
import json
import os
import shutil
from collections import OrderedDict

from dateparser_scripts.utils import get_raw_data

DIGIT_PATTERN = re.compile(r'^\d*$')

os.chdir(os.path.dirname(os.path.abspath(__file__)))
get_raw_data()

cldr_rbnf_dir = "../raw_data/cldr_rbnf/rbnf/"


def _get_numeral_data(language):
    with open(cldr_rbnf_dir + language + '.json') as f:
        cldr_rbnf_data = json.load(f)
    spellout_dict = cldr_rbnf_data["rbnf"]["rbnf"].get("SpelloutRules")
    numeral_dict = OrderedDict()

    if spellout_dict:
        spellout_keys = sorted(spellout_dict.keys())
        for spellout_key in spellout_keys:
            spellout_key_dict = spellout_dict[spellout_key]
            num_keys = sorted([int(key) for key in spellout_key_dict.keys()
                              if DIGIT_PATTERN.match(key)])
            numeral_dict[spellout_key] = OrderedDict()
            for i in range(0, len(num_keys)-1):
                if num_keys[i+1] == num_keys[i] + 1:
                    numeral_dict[spellout_key][str(num_keys[i])] = spellout_key_dict[str(num_keys[i])]
                else:
                    num_range = (num_keys[i], num_keys[i+1]-1)
                    numeral_dict[spellout_key][str(num_range)] = spellout_key_dict[str(num_keys[i])]
            numeral_dict[spellout_key][str((num_keys[len(num_keys)-1], "inf"))] = spellout_key_dict[
                str(num_keys[len(num_keys)-1])]

    return numeral_dict


def main():
    parent_directory = "../dateparser_data/cldr_language_data"
    directory = "../dateparser_data/cldr_language_data/numeral_translation_data/"
    if not os.path.isdir(parent_directory):
        os.mkdir(parent_directory)
    if os.path.isdir(directory):
        shutil.rmtree(directory)
    os.mkdir(directory)

    rbnf_languages = [filename[:-5] for filename in os.listdir(cldr_rbnf_dir)]

    for language in rbnf_languages:
        numeral_dict = _get_numeral_data(language)
        if numeral_dict:
            filename = directory + language + ".json"
            print("writing " + filename)
            json_string = json.dumps(numeral_dict, indent=4, separators=(',', ': '),
                                     ensure_ascii=False).encode('utf-8')
            with open(filename, 'wb') as f:
                f.write(json_string)


if __name__ == '__main__':
    main()
