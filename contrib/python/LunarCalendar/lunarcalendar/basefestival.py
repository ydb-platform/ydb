# -*- coding: utf-8 -*-
from __future__ import unicode_literals


def zh_map(lang):
    lang = lang.lower()
    return {'zh': 'zh_hans',
            'zh-cn': 'zh_hans',
            'zh_cn': 'zh_hans',
            'zh-hans': 'zh_hans',
            'zh-tw': 'zh_hant',
            'zh_tw': 'zh_hant',
            'zh-hant': 'zh_hant',
            }.get(lang, lang)


class Festival(object):
    def __init__(self, date_func, **langs):
        '''
        :param date_func: a function which recieves a paramater called `year`,
            then return a `datetime.date()` instance.
        :param langs: language dict, such as en, zh, and more.
        '''
        self.date_func = date_func
        self.langs = {}
        # langs's format like below:
        # self.langs['en'] = [xxxx, xxxx, ...]
        # self.langs['zh_hans'] = [xxxx, xxxx, ...]
        # self.langs['zh_hant'] = [xxxx, xxxx, ...]
        for origin_key in langs:
            new_key = zh_map(origin_key)
            self.langs[new_key] = langs[origin_key].split(',')

    def get_lang(self, lang):
        ''' get the first name in the target language '''
        lang = zh_map(lang)
        if lang not in self.langs:
            raise NotImplemented
        return self.langs[lang][0]

    def get_lang_list(self, lang):
        ''' get the name list in the target language '''
        lang = zh_map(lang)
        if lang not in self.langs:
            raise NotImplemented
        return self.langs[lang]

    def __call__(self, year):
        return self.date_func(year)
