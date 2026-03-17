from os import path

import pytest

from pycbrf import Banks


@pytest.mark.xfail
def test_get_archive():
    assert Banks._get_data_swift()


@pytest.mark.parametrize('legacy', [True, False])
def test_banks(legacy, monkeypatch, datafix_readbin):

    @classmethod  # hack
    def get_archive(cls, url):
        if legacy:

            basename = path.basename(url)

            if basename == 'bik_swift-bik.zip':
                return datafix_readbin(basename, io=True)

            return datafix_readbin('bik_db_28062018.zip', io=True)

        return datafix_readbin('20201204ED01OSBR.zip', io=True)

    monkeypatch.setattr(Banks, '_get_archive', get_archive)

    banks = Banks('2018-06-29' if legacy else '2020-11-04')

    bank0 = banks['dummy']
    bank = banks['045004641']

    if legacy:
        assert bank.place == 'НОВОСИБИРСК'
        assert bank.place_type.shortname == 'Г'
        assert bank.region.name == 'НОВОСИБИРСКАЯ ОБЛАСТЬ'

    else:
        assert bank.place == 'Новосибирск'
        assert bank.place_type == 'г'
        assert bank.region_code == '50'

    bank_swift = banks['SABRRUMMNH1']  # by swift bic
    if bank_swift:
        assert bank_swift.bic == '045004641'

    annotated = Banks.annotate([bank, bank0])[0]
    assert annotated['БИК'] == '045004641'
    assert 'vkey' not in annotated['Тип']

    if not legacy:

        bank = banks['044525487']

        # Test restrictions
        assert bank.restricted
        assert len(bank.restrictions) == 2

        annotated = Banks.annotate([bank])[0]
        assert annotated['Ограничения']
        assert annotated['Счета']

        # Test accounts
        assert bank.accounts
        account = bank.accounts[0]
        assert account.number == '30101810400000000487'
        assert 'CRSA' in f'{account}'
        assert len(account.restrictions) == 2
