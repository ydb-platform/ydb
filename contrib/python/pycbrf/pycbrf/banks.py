import re
from datetime import datetime
from io import BytesIO
from logging import getLogger
from typing import NamedTuple, List, Union, Optional, Set
from xml.etree import ElementTree
from zipfile import ZipFile

from dbf_light import Dbf

from .exceptions import PycbrfException
from .utils import WithRequests

LOG = getLogger(__name__)


class BankLegacy(NamedTuple):
    """Represents bank entry in legacy format.

    Such objects will populate Banks().banks

    """
    bic: str
    name: str
    name_full: str
    region_code: str
    region: NamedTuple
    zip: str
    place_type: NamedTuple
    place: str
    address: str
    rkc_bic: str
    term: int
    date_added: datetime.date
    date_updated: Optional[datetime.date]
    date_change: Optional[datetime.date]
    mfo: str
    corr: str
    corr_bik: str
    phone: str
    telegraph: str
    commutator: str
    okpo: str
    regnum: str
    type: NamedTuple
    pay_type: NamedTuple
    control_code: str
    control_date: Optional[datetime.date]
    swift: Optional[str]


class Bank(NamedTuple):
    """Represents bank entry in current format.

    Such objects will populate Banks().banks

    """
    bic: str
    name_full: str
    name_full_eng: str
    region_code: str
    country_code: str
    zip: str
    place_type: str
    place: str
    address: str
    date_added: datetime
    corr: str
    regnum: str
    type: str
    swift: str
    restricted: bool
    restrictions: List['Restriction']
    accounts: List['Account']


class Account:
    """Represents an account."""

    types = {
        'CBRA': 'Счёт Банка России',
        'CRSA': 'Корреспондентский счёт',
        'BANA': 'Банковский счёт',
        'TRSA': 'Счёт Федерального казначейства',
        'TRUA': 'Счёт доверительного управления',
        'CLAC': 'Клиринговый счёт',
        'UTRA': 'Единый казначейский счёт',
    }

    __slots__ = ['number', 'type', 'type_str', 'date_added', 'date_removed', 'restrictions']

    def __init__(
        self,
        *,
        number: str,
        type: str,
        date_added: Optional[datetime.date],
        date_removed: Optional[datetime.date],
        restrictions: List['Restriction'],
    ):
        self.number = number
        self.type = type
        self.type_str = self.types.get(type, '')
        self.date_added = date_added
        self.date_removed = date_removed
        self.restrictions = restrictions

    def __str__(self):
        dates = f'{self.date_added or ""}'
        if dates:
            dates = f'[{dates}]'
        return f'{self.number} [{self.type}] {self.type_str} {dates}'


class Restriction:
    """Represents a restriction imposed on an institution."""

    __slots__ = ['code', 'date', 'account', 'title']

    codes = {
        'URRS': 'Ограничение предоставления сервиса срочного перевода',
        'LWRS': 'Отзыв (аннулирование) лицензии',
        'MRTR': 'Мораторий на удовлетворение требований кредиторов',
        'LMRS': 'Временное сохранение счета с его функционированием в ограниченном режиме',
        'CLRS': 'Закрытие счета',
        'FPRS': 'Приостановление предоставления сервиса быстрых платежей',
    }
    """УФЭБС_2021_1_1_КБР_Кодовые_Значения.pdf
    81 Статус участника
    82 Ограничения операций по счету

    """

    def __init__(self, *, code: str, date: datetime.date, account: str = ''):
        self.code = code

        self.date = date

        self.account = account
        """Might be empty in not an account level restriction."""

        self.title = self.codes.get(code, '')

    def __str__(self):
        return f'{self.date} {self.code} [{self.account}] {self.title}'


class Banks(WithRequests):

    def __init__(self, on_date: Union[datetime, str] = None):
        """Fetches BIC data.

        :param on_date: Date to get data for.
            Python date objects and ISO date string are supported.
            If not set data for today will be fetched.

        """
        if isinstance(on_date, str):
            on_date = datetime.strptime(on_date, '%Y-%m-%d')

        on_date = on_date or datetime.now()
        legacy = on_date < datetime(2018, 7, 1)

        self.banks: Union[List['Bank'], List['BankLegacy']] = self._get_data(on_date=on_date, legacy=legacy)
        self.on_date = on_date
        self.legacy = legacy  # CB RF radically changed format from DBF (legacy) to XML.

    def __getitem__(self, item: str) -> Optional[Union['Bank', 'BankLegacy']]:

        key = 'swift' if len(item) in {8, 11} else 'bic'
        indexed = {getattr(bank, key): bank for bank in self.banks}

        return indexed.get(item)

    @classmethod
    def get_titles(cls) -> dict:
        """Returns fields titles."""

        titles = {
            'bic': 'БИК',  # Банковский идентификационный код
            'swift': 'Код SWIFT',
            'name': 'Название',
            'name_full': 'Полное название',
            'name_full_eng': 'Полное название (англ.)',

            'date_added': 'Дата добавления записи',
            'date_updated': 'Дата обновления записи',
            'date_change': 'Дата изменения реквизитов',

            'restricted': 'С ограничениями',  # Fuzzy analogy for `control_code`.
            'restrictions': 'Ограничения',
            'control_code': 'Код контроля',
            'control_date': 'Дата контроля',

            'accounts': 'Счета',
            'corr': 'Кор. счёт',
            'corr_bik': 'Кор. счёт (расчёты с БИК)',

            'regnum': 'Регистрационный номер',
            'mfo': 'Номер МФО',
            'okpo': 'Номер ОКПО',  # Классификатор предприятий и организаций
            'type': 'Тип',
            'pay_type': 'Тип расчётов',

            'country_code': 'Код страны',
            'region_code': 'Код региона ОКАТО',  # Классификатор объектов административно-территориального деления
            'region': 'Регион',
            'zip': 'Индекс',
            'place_type': 'Тип населённого пункта',
            'place': 'Населённый пункт',
            'address': 'Адрес',

            'phone': 'Телефон',
            'telegraph': 'Телеграф',
            'commutator': 'Коммутатор',

            'rkc_bic': 'БИК РКЦ',  # Рассчётно-кассовый центр
            'term': 'Срок проведения расчётов (дней)',
        }
        return titles

    @classmethod
    def annotate(cls, banks: List['Bank']) -> List[dict]:
        """Annotates bank objects with titles.

        :param banks: A list of Bank objects to annotate.

        """
        titles = cls.get_titles()
        annotated = []

        def pick_value(in_dict):
            for key in ['name', 'fullname', 'uername']:
                val = in_dict.get(key)
                if val:
                    return val
            return '<no name>'

        unset = object()

        for bank in banks:

            if not bank:
                continue

            bank_dict = {}
            bank = bank._asdict()

            for alias, title in titles.items():
                value = bank.get(alias, unset)

                if value is unset:
                    # Some fields may be missing in Bank/BankLegacy
                    continue

                if isinstance(value, (BankLegacy, Bank)):
                    value = pick_value(value._asdict())

                elif isinstance(value, bool):
                    value = 'Да' if value else 'Нет'

                elif isinstance(value, list):
                    value = '\n  ' + '\n  '.join(map(str, value))

                bank_dict[title] = value or ''

            annotated.append(bank_dict)

        return annotated

    @classmethod
    def _get_archive(cls, url: str) -> Optional[BytesIO]:
        LOG.debug(f'Fetching data from {url} ...')

        response = cls._get_response(url, stream=True)

        if response.status_code != 200:
            # E.g. 404 is expected on weekends.
            return None

        return BytesIO(response.content)

    @classmethod
    def _read_zipped_xml(cls, zipped: BytesIO) -> ElementTree:

        with ZipFile(zipped, 'r') as zip_:
            filename = zip_.namelist()[0]

            with zip_.open(filename) as f:
                return ElementTree.fromstring(f.read())

    @classmethod
    def _get_data(cls, on_date: datetime, legacy: bool = False) -> Union[List['Bank'], List['BankLegacy']]:

        if legacy:
            return cls._get_data_dbf(on_date=on_date)

        return cls._get_data_xml(on_date=on_date)

    @classmethod
    def _get_data_xml(cls, on_date: datetime) -> List['Bank']:
        """Справочник БИК (Клиентов Банка России). XML ED807

        https://cbr.ru/development/Formats/

        :param on_date:

        """
        data = cls._get_archive(f"http://www.cbr.ru/VFS/mcirabis/BIKNew/{on_date.strftime('%Y%m%d')}ED01OSBR.zip")

        if data is None:
            return []

        xml = cls._read_zipped_xml(data)

        def parse_date(val):
            if not val:
                return val
            return datetime.strptime(val, '%Y-%m-%d').date()

        ns = '{urn:cbr-ru:ed:v2.0}'

        types = {
            '00': 'Главное управление Банка России',
            '10': 'Расчетно-кассовый центр',
            '12': 'Отделение, отделение – национальный банк главного управления Банка России',
            '15': 'Структурное подразделение центрального аппарата Банка России',
            '16': 'Кассовый центр',
            '20': 'Кредитная организация',
            '30': 'Филиал кредитной организации',
            '40': 'Полевое учреждение Банка России',
            '51': 'Федеральное казначейство',
            '52': 'Территориальный орган Федерального казначейства',
            '60': 'Иностранная кредитная организация',
            '65': 'Иностранный центральный (национальный) банк',
            '71': 'Клиент кредитной организации, являющийся косвенным участником',
            '75': 'Клиринговая организация',
            '78': 'Внешняя платежная система',
            '90': 'Конкурсный управляющий (ликвидатор, ликвидационная комиссия)',
            '99': 'Клиент Банка России, не являющийся участником платежной системы',
        }
        """УФЭБС_2021_1_1_КБР_Кодовые_Значения.pdf
        77 Тип участника перевода
        
        """

        banks = []

        for entry in xml.findall(f'{ns}BICDirectoryEntry'):
            restrictions_applied = []

            bic = entry.attrib['BIC']

            el_info = entry.find(f'{ns}ParticipantInfo')
            attrs_info = el_info.attrib

            if attrs_info['ParticipantStatus'] == 'PSDL':  # Маркер удаления
                continue

            for el_restriction in el_info.findall(f'{ns}RstrList'):
                attrs = el_restriction.attrib
                code = attrs['Rstr']
                restrictions_applied.append(Restriction(
                    code=code,
                    date=parse_date(attrs['RstrDate']),
                ))

            swiftcode = None

            for el_swift in entry.findall(f'{ns}SWBICS'):
                if el_swift.attrib.get('DefaultSWBIC'):
                    swiftcode = el_swift.attrib['SWBIC']  # [8/11]
                    break

            account_corr_number = ''
            accounts = []

            for el_account in entry.findall(f'{ns}Accounts'):
                attrs = el_account.attrib

                if attrs['AccountStatus'] == 'ACDL':  # [4]  Маркер удаления
                    continue

                account_type = attrs['RegulationAccountType']
                account_number = attrs['Account']  # [20]

                if account_type == 'CRSA':
                    account_corr_number = account_number

                account_restrictions = []

                for el_restriction in el_account.findall(f'{ns}AccRstrList'):
                    attrs = el_restriction.attrib
                    restriction = Restriction(
                        code=attrs['AccRstr'],
                        date=parse_date(attrs['AccRstrDate']),
                        account=account_number,
                    )
                    restrictions_applied.append(restriction)
                    account_restrictions.append(restriction)

                accounts.append(Account(
                    number=account_number,
                    type=account_type,
                    date_added=parse_date(attrs.get('DateIn')),
                    date_removed=parse_date(attrs.get('DateOut')),
                    restrictions=account_restrictions,
                ))

            banks.append(Bank(
                bic=bic,  # [9]
                name_full=attrs_info['NameP'],  # [160]
                name_full_eng=attrs_info.get('EnglName', ''),  # [140]
                region_code=attrs_info['Rgn'],  # [2] 00 - за пределами РФ
                country_code=attrs_info.get('CntrCd', ''),  # [2]
                zip=attrs_info.get('Ind', ''),  # [6]
                place_type=attrs_info.get('Tnp', ''),  # [5]
                place=attrs_info.get('Nnp', ''),  # [25]
                address=attrs_info.get('Adr', ''),  # [160]
                regnum=attrs_info.get('RegN', ''),  # [9]
                type=types.get(attrs_info['PtType'], ''),  # [2]
                date_added=parse_date(attrs_info['DateIn']),
                corr=account_corr_number,
                swift=swiftcode,
                restricted=bool(restrictions_applied),
                restrictions=restrictions_applied,
                accounts=accounts,
            ))

        return banks

    @classmethod
    def _read_zipped_db(cls, zipped: BytesIO, filename: str):
        with Dbf.open_zip(filename, zipped, case_sensitive=False) as dbf:
            for row in dbf:
                yield row

    @classmethod
    def _get_data_dbf(cls, on_date: datetime) -> List['BankLegacy']:

        try:
            swifts = cls._get_data_swift()

        except PycbrfException:
            swifts = {}

        zipped = cls._get_archive(
            f"http://www.cbr.ru/vfs/mcirabis/BIK/bik_db_{on_date.strftime('%d%m%Y')}.zip")

        if zipped is None:
            return []

        def get_indexed(dbname, index):
            return {getattr(region, index): region for region in cls._read_zipped_db(zipped, filename=dbname)}

        regions = get_indexed('reg.dbf', 'rgn')
        types = get_indexed('pzn.dbf', 'pzn')
        place_types = get_indexed('tnp.dbf', 'tnp')
        pay_types = get_indexed('uer.dbf', 'uer')

        banks = []

        for row in cls._read_zipped_db(zipped, filename='bnkseek.dbf'):
            region_code = row.rgn
            bic = row.newnum

            telegraph = []
            row.at1 and telegraph.append(row.at1)
            row.at2 and telegraph.append(row.at2)

            term = row.srok or 0

            if term:
                term = int(term)

            control_code = row.real
            """
            БЛОК - прекращенией операций из-за блокировки
            ЗСЧТ - прекращенией операций из-за закрытия счёта филиала
            ИЗМР - прекращенией операций из-за изменения реквизитов
            ИНФО - предвариательное оповещение о скором прекращении операций
            ИСКЛ - предвариательное оповещение о начале процесса ликвизации
            ЛИКВ - говорит о создании ликвидационной комиссии
            ОТЗВ - отзыв лицензии, прекращение операций
            ВРФС - режим временного функционирование счёта
            """

            banks.append(BankLegacy(
                bic=bic,
                name=row.namen,
                name_full=row.namep,
                region_code=region_code,
                region=regions.get(region_code),
                zip=row.ind,
                place_type=place_types.get(row.tnp),
                place=row.nnp,
                address=row.adr,
                rkc_bic=row.rkc,
                term=term,
                date_added=row.date_in,
                date_updated=row.dt_izm,
                date_change=row.dt_izmr,
                mfo=row.permfo,
                corr=row.ksnp,
                corr_bik=row.newks,
                phone=row.telef,
                telegraph=','.join(telegraph),
                commutator=row.cks,
                okpo=row.okpo,
                regnum=row.regn,
                type=types[row.pzn],
                pay_type=pay_types[row.uer],
                control_code=control_code,
                control_date=row.date_ch,
                swift=swifts.get(bic),
            ))

        return banks

    @classmethod
    def _get_data_swift(cls) -> dict:
        # At some moment static URL has became dynamic, and now ne need to search for it every time.
        host = 'http://www.cbr.ru'
        response = cls._get_response(f'{host}/analytics/digest/')

        found = re.findall(r'href="([^."]+\.zip)"', response.text)

        if not found or len(found) > 1:
            raise PycbrfException('Unable to get SWIFT info archive link')

        url = host + found[0]

        items = {
            item.kod_rus: item.kod_swift for item in
            cls._read_zipped_db(cls._get_archive(url), filename='bik_swif.dbf')
        }
        return items
