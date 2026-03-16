# Copyright 2014 Facebook, Inc.

# You are hereby granted a non-exclusive, worldwide, royalty-free license to
# use, copy, modify, and distribute this software in source code or binary
# form for use in connection with the web services and APIs provided by
# Facebook.

# As with any software that integrates with the Facebook platform, your use
# of this software is subject to the Facebook Developer Principles and
# Policies [http://developers.facebook.com/policy/]. This copyright notice
# shall be included in all copies or substantial portions of the software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.

import pprint
import six

from facebook_business.adobjects.serverside.gender import Gender
from facebook_business.adobjects.serverside.normalize import Normalize


class UserData(object):
    multi_value_constructor_err = 'Cannot set both {} and {} parameters via constructor. Please set either the singular or plural parameter, not both.'

    param_types = {
        'emails': 'list[str]',
        'phones': 'list[str]',
        'genders': 'list[Gender]',
        'dates_of_birth': 'list[str]',
        'last_names': 'list[str]',
        'first_names': 'list[str]',
        'cities': 'list[str]',
        'states': 'list[str]',
        'country_codes': 'list[str]',
        'zip_codes': 'list[str]',
        'external_ids': 'list[str]',
        'client_ip_address': 'str',
        'client_user_agent': 'str',
        'fbc': 'str',
        'fbp': 'str',
        'subscription_id': 'str',
        'fb_login_id': 'str',
        'lead_id': 'str',
        'f5first': 'str',
        'f5last': 'str',
        'fi': 'str',
        'dobd': 'str',
        'dobm': 'str',
        'doby': 'str',
        'madid': 'str',
        'anon_id': 'str',
        'ctwa_clid': 'ctwa_clid',
        'page_id': 'page_id',
    }

    def __init__(
        self,
        email=None,
        phone=None,
        gender=None,
        date_of_birth=None,
        last_name=None,
        first_name=None,
        city=None,
        state=None,
        country_code=None,
        zip_code=None,
        external_id=None,
        client_ip_address=None,
        client_user_agent=None,
        fbc=None,
        fbp=None,
        subscription_id=None,
        fb_login_id=None,
        lead_id=None,
        f5first=None,
        f5last=None,
        fi=None,
        dobd=None,
        dobm=None,
        doby=None,
        emails=None,
        phones=None,
        genders=None,
        dates_of_birth=None,
        last_names=None,
        first_names=None,
        cities=None,
        states=None,
        country_codes=None,
        zip_codes=None,
        external_ids=None,
        madid=None,
        anon_id=None,
        ctwa_clid=None,
        page_id=None,
    ):
        # type: (str, str, Gender, str, str, str, str, str, str, str, str, str, str, str, str, str, str, str, str, str, str, str, str, str, list[str], list[str], list[Gender], list[str], list[str], list[str], list[str], list[str], list[str], list[str], list[str], str, str, str, str) -> None

        """UserData is a set of identifiers Facebook can use for targeted attribution"""
        self._emails = None
        self._phones = None
        self._genders = None
        self._dates_of_birth = None
        self._last_names = None
        self._first_names = None
        self._cities = None
        self._states = None
        self._country_codes = None
        self._zip_codes = None
        self._external_ids = None
        self._client_ip_address = None
        self._client_user_agent = None
        self._fbc = None
        self._fbp = None
        self._subscription_id = None
        self._fb_login_id = None
        self._lead_id = None
        self._f5first = None
        self._f5last = None
        self._fi = None
        self._dobd = None
        self._dobm = None
        self._doby = None
        self._madid = None
        self._anon_id = None
        self._ctwa_clid = None
        self._page_id = None

        if email is not None and emails is not None:
            raise ValueError(UserData.multi_value_constructor_err.format('email', 'emails'))
        if phone is not None and phones is not None:
            raise ValueError(UserData.multi_value_constructor_err.format('phone', 'phones'))
        if gender is not None and genders is not None:
            raise ValueError(UserData.multi_value_constructor_err.format('gender', 'genders'))
        if date_of_birth is not None and dates_of_birth is not None:
            raise ValueError(UserData.multi_value_constructor_err.format('date_of_birth', 'dates_of_birth'))
        if last_name is not None and last_names is not None:
            raise ValueError(UserData.multi_value_constructor_err.format('last_name', 'last_names'))
        if first_name is not None and first_names is not None:
            raise ValueError(UserData.multi_value_constructor_err.format('first_name', 'first_names'))
        if city is not None and cities is not None:
            raise ValueError(UserData.multi_value_constructor_err.format('city', 'cities'))
        if state is not None and states is not None:
            raise ValueError(UserData.multi_value_constructor_err.format('state', 'states'))
        if country_code is not None and country_codes is not None:
            raise ValueError(UserData.multi_value_constructor_err.format('country_code', 'country_codes'))
        if zip_code is not None and zip_codes is not None:
            raise ValueError(UserData.multi_value_constructor_err.format('zip_code', 'zip_codes'))
        if external_id is not None and external_ids is not None:
            raise ValueError(UserData.multi_value_constructor_err.format('external_id', 'external_ids'))

        if email is not None:
            self.email = email
        elif emails is not None:
            self.emails = emails
        if phone is not None:
            self.phone = phone
        elif phones is not None:
            self.phones = phones
        if gender is not None:
            self.gender = gender
        elif genders is not None:
            self.genders = genders
        if date_of_birth is not None:
            self.date_of_birth = date_of_birth
        elif dates_of_birth is not None:
            self.dates_of_birth = dates_of_birth
        if last_name is not None:
            self.last_name = last_name
        elif last_names is not None:
            self.last_names = last_names
        if first_name is not None:
            self.first_name = first_name
        elif first_names is not None:
            self.first_names = first_names
        if city is not None:
            self.city = city
        elif cities is not None:
            self.cities = cities
        if state is not None:
            self.state = state
        elif states is not None:
            self.states = states
        if country_code is not None:
            self.country_code = country_code
        elif country_codes is not None:
            self.country_codes = country_codes
        if zip_code is not None:
            self.zip_code = zip_code
        elif zip_codes is not None:
            self.zip_codes = zip_codes
        if external_id is not None:
            self.external_id = external_id
        elif external_ids is not None:
            self.external_ids = external_ids

        if client_ip_address is not None:
            self.client_ip_address = client_ip_address
        if client_user_agent is not None:
            self.client_user_agent = client_user_agent
        if fbc is not None:
            self.fbc = fbc
        if fbp is not None:
            self.fbp = fbp
        if subscription_id is not None:
            self.subscription_id = subscription_id
        if fb_login_id is not None:
            self.fb_login_id = fb_login_id
        if lead_id is not None:
            self.lead_id = lead_id
        if f5first is not None:
            self.f5first = f5first
        if f5last is not None:
            self.f5last = f5last
        if fi is not None:
            self.fi = fi
        if dobd is not None:
            self.dobd = dobd
        if dobm is not None:
            self.dobm = dobm
        if doby is not None:
            self.doby = doby
        if madid is not None:
            self.madid = madid
        if anon_id is not None:
            self.anon_id = anon_id
        if ctwa_clid is not None:
            self.ctwa_clid = ctwa_clid
        if page_id is not None:
            self.page_id = page_id

    @property
    def email(self):
        """Gets the email.

        An email address, in lowercase.

        :return: The email.
        :rtype: str
        """
        return self._emails[0] if self._emails else None

    @email.setter
    def email(self, email):
        """Sets the email.

        An email address, in lowercase.

        :param email: The email.
        :type: str
        """

        self._emails = [email] if email is not None else None

    @property
    def emails(self):
        """Gets the emails.

        A list of email addresses, in lowercase.

        :return: The emails.
        :rtype: list[str]
        """
        return self._emails

    @emails.setter
    def emails(self, emails):
        """Sets the emails.

        A list of email addresses, in lowercase.

        :param emails: A list of emails.
        :type: list[str]
        """

        self._emails = emails

    @property
    def phone(self):
        """Gets the phone.

        A phoneone number. Include only digits with country code, area code, and number.

        :return: The phone.
        :rtype: str
        """
        return self._phones[0] if self._phones else None

    @phone.setter
    def phone(self, phone):
        """Sets the phone.

        A phone number. Include only digits with country code, area code, and number.

        :param phone: The phone.
        :type: str
        """

        self._phones = [phone] if phone is not None else None

    @property
    def phones(self):
        """Gets the phones.

        A list of phone numbers. Include only digits with country code, area code, and number.

        :return: The phone numbers.
        :rtype: list[str]
        """
        return self._phones

    @phones.setter
    def phones(self, phones):
        """Sets the phones.

        A list of phone numbers. Include only digits with country code, area code, and number.

        :param phones: A list of phones.
        :type: list[str]
        """

        self._phones = phones

    @property
    def gender(self):
        """Gets the gender.

        Gender, in lowercase. Either f or m.

        :return: The gender.
        :rtype: Gender
        """
        return self._genders[0] if self._genders else None

    @gender.setter
    def gender(self, gender):
        """Sets the gender.

        Gender, in lowercase. Either f or m.

        :param gender: The gender.
        :type: Gender
        """
        if gender is None:
            return
        if not isinstance(gender, Gender):
            raise TypeError('UserData.gender must be of type Gender')

        self._genders = [gender]

    @property
    def genders(self):
        """Gets the genders.

        A list of genders, in lowercase. Either f or m.

        :return: A list of genders.
        :rtype: list[Gender]
        """
        return self._genders

    @genders.setter
    def genders(self, genders):
        """Sets the genders.

        A list of Genders, in lowercase. Either f or m.

        :param genders: The genders.
        :type: Gender
        """
        if genders and not (all(isinstance(gender, Gender) for gender in genders)):
            raise TypeError('UserData.genders must be of type list[Gender]')

        self._genders = genders

    @property
    def date_of_birth(self):
        """Gets the date of birth.

        A date of birth given as YYYYMMDD.


        :return: The date of birth.
        :rtype: str
        """
        return self._dates_of_birth[0] if self._dates_of_birth else None

    @date_of_birth.setter
    def date_of_birth(self, date_of_birth):
        """Sets the date of birth.

        A date of birth given as YYYYMMDD.

        :param date_of_birth: The date of birth.
        :type: str
        """

        self._dates_of_birth = [date_of_birth] if date_of_birth is not None else None

    @property
    def dates_of_birth(self):
        """Gets the dates of birth.

        A list of dates of birth given as YYYYMMDD.


        :return: The dates of birth.
        :rtype: list[str]
        """
        return self._dates_of_birth

    @dates_of_birth.setter
    def dates_of_birth(self, dates_of_birth):
        """Sets the dates of birth.

        A list of dates of birth given as YYYYMMDD.

        :param dates_of_birth: The dates of birth.
        :type: list[str]
        """

        self._dates_of_birth = dates_of_birth

    @property
    def last_name(self):
        """Gets the last_name.

        A last name in lowercase.

        :return: The last name.
        :rtype: str
        """
        return self._last_names[0] if self._last_names else None

    @last_name.setter
    def last_name(self, last_name):
        """Sets the last name.

        A last name in lowercase.

        :param last_name: The last name.
        :type: str
        """

        self._last_names = [last_name] if last_name is not None else None

    @property
    def last_names(self):
        """Gets the last_names.

        A list of last names in lowercase.

        :return: The last names.
        :rtype: list[str]
        """
        return self._last_names

    @last_names.setter
    def last_names(self, last_names):
        """Sets the last names.

        A list of last names in lowercase.

        :param last_names: The last names.
        :type: list[str]
        """

        self._last_names = last_names

    @property
    def first_name(self):
        """Gets the first name.

        A first name in lowercase.

        :return: The first name.
        :rtype: str
        """
        return self._first_names[0] if self._first_names else None

    @first_name.setter
    def first_name(self, first_name):
        """Sets the first name.

        A first name in lowercase.

        :param first_name: The first name.
        :type: str
        """

        self._first_names = [first_name] if first_name is not None else None

    @property
    def first_names(self):
        """Gets the first names.

        A list of first names in lowercase.

        :return: The first names.
        :rtype: list[str]
        """
        return self._first_names

    @first_names.setter
    def first_names(self, first_names):
        """Sets the first names.

        A list of first names in lowercase.

        :param first_names: The first names.
        :type: list[str]
        """

        self._first_names = first_names

    @property
    def city(self):
        """Gets the city.

        A city in lower-case without spaces or punctuation.

        :return: The city.
        :rtype: str
        """
        return self._cities[0] if self._cities else None

    @city.setter
    def city(self, city):
        """Sets the city.

        A city in lower-case without spaces or punctuation.

        :param city: The city.
        :type: str
        """

        self._cities = [city] if city is not None else None

    @property
    def cities(self):
        """Gets the cities.

        A list of cities in lower-case without spaces or punctuation.

        :return: The cities.
        :rtype: list[str]
        """
        return self._cities

    @cities.setter
    def cities(self, cities):
        """Sets the cities.

        A list of cities in lower-case without spaces or punctuation.

        :param cities: The cities.
        :type: list[str]
        """

        self._cities = cities

    @property
    def state(self):
        """Gets the state.

        A two-letter state code in lowercase.

        :return: The state.
        :rtype: str
        """
        return self._states[0] if self._states else None

    @state.setter
    def state(self, state):
        """Sets the state.

        A two-letter state code in lowercase.

        :param state: The state.
        :type: str
        """

        self._states = [state] if state is not None else None

    @property
    def states(self):
        """Gets the states.

        A list of two-letter state codes in lowercase.

        :return: The states.
        :rtype: list[str]
        """
        return self._states

    @states.setter
    def states(self, states):
        """Sets the states.

        A list of two-letter state codes in lowercase.

        :param states: The states.
        :type: list[str]
        """

        self._states = states

    @property
    def country_code(self):
        """Gets the country code.

         A two-letter country code in lowercase

        :return: The country code.
        :rtype: str
        """
        return self._country_codes[0] if self._country_codes else None

    @country_code.setter
    def country_code(self, country_code):
        """Sets a two-letter country code in lowercase.

        :param country_code: The country code
        :type: str
        """

        self._country_codes = [country_code] if country_code is not None else None

    @property
    def country_codes(self):
        """Gets the country codes.

         A list of two-letter country codes in lowercase

        :return: The country codes.
        :rtype: list[str]
        """
        return self._country_codes

    @country_codes.setter
    def country_codes(self, country_codes):
        """Sets a list of two-letter country codes in lowercase.

        :param country_codes: The country codes
        :type: list[str]
        """

        self._country_codes = country_codes

    @property
    def zip_code(self):
        """Gets the zipcode.

        TFor the United States, this is a five-digit zip code.
        For other locations, follow each country's standards.

        :return: The zipcode.
        :rtype: str
        """
        return self._zip_codes[0] if self._zip_codes else None

    @zip_code.setter
    def zip_code(self, zip_code):
        """Sets the zipcode.

        For the United States, this is a five-digit zip code.
        For other locations, follow each country's standards.

        :param zip_code: The zipcode.
        :type: str
        """

        self._zip_codes = [zip_code] if zip_code is not None else None

    @property
    def zip_codes(self):
        """Gets the zipcodes.

        For the United States, this is a list of five-digit zip codes.
        For other locations, follow each country's standards.

        :return: The zipcodes.
        :rtype: list[str]
        """
        return self._zip_codes

    @zip_codes.setter
    def zip_codes(self, zip_codes):
        """Sets the zipcodes.

        For the United States, this is a list of five-digit zip codes.
        For other locations, follow each country's standards.

        :param zip_codes: The zipcodes.
        :type: list[str]
        """

        self._zip_codes = zip_codes

    @property
    def external_id(self):
        """Gets the external id.

        Any unique ID from the advertiser, such as loyalty membership IDs, user IDs, and external cookie IDs.
        In the Offline Conversions API (https://www.facebook.com/business/help/104039186799009),
        this is known as extern_id. For more information, see Offline Conversions, Providing External IDs. If
        External ID is being sent via other channels, then it should be sent in the same format via the Conversions API.

        :return: The external id.
        :rtype: str
        """
        return self._external_ids[0] if self._external_ids else None

    @external_id.setter
    def external_id(self, external_id):
        """Sets the external id.

        Any unique ID from the advertiser, such as loyalty membership IDs, user IDs, and external cookie IDs.
        In the Offline Conversions API (https://www.facebook.com/business/help/104039186799009),
        this is known as extern_id. For more information, see Offline Conversions, Providing External IDs. If
        External ID is being sent via other channels, then it should be sent in the same format via the Conversions API.

        :param external_id: The external id.
        :type: str
        """

        self._external_ids = [external_id] if external_id is not None else None

    @property
    def external_ids(self):
        """Gets the external ids.

        A list of any unique IDs from the advertiser, such as loyalty membership IDs, user IDs, and external cookie IDs.
        In the Offline Conversions API (https://www.facebook.com/business/help/104039186799009),
        this is known as extern_id. For more information, see Offline Conversions, Providing External IDs. If
        External ID is being sent via other channels, then it should be sent in the same format via the Conversions API.

        :return: The external ids.
        :rtype: list[str]
        """
        return self._external_ids

    @external_ids.setter
    def external_ids(self, external_ids):
        """Sets the external ids.

        A list of any unique IDs from the advertiser, such as loyalty membership IDs, user IDs, and external cookie IDs.
        In the Offline Conversions API (https://www.facebook.com/business/help/104039186799009),
        this is known as extern_id. For more information, see Offline Conversions, Providing External IDs. If
        External ID is being sent via other channels, then it should be sent in the same format via the Conversions API.

        :param external_ids: The external ids.
        :type: list[str]
        """

        self._external_ids = external_ids

    @property
    def client_ip_address(self):
        """Gets the client ip address.

        The IP address of the browser corresponding to the event.

        :return: The client ip address.
        :rtype: str
        """
        return self._client_ip_address

    @client_ip_address.setter
    def client_ip_address(self, client_ip_address):
        """Sets the client ip address.

        The IP address of the browser corresponding to the event.

        :param client_ip_address: The client ip address.
        :type: str
        """

        self._client_ip_address = client_ip_address

    @property
    def client_user_agent(self):
        """Gets the client user agent.

        The user agent for the browser corresponding to the event.

        :return: The client user agent.
        :rtype: str
        """
        return self._client_user_agent

    @client_user_agent.setter
    def client_user_agent(self, client_user_agent):
        """Sets the client user agent.

        The user agent for the browser corresponding to the event.

        :param client_user_agent: The client user agent.
        :type: str
        """

        self._client_user_agent = client_user_agent

    @property
    def fbc(self):
        """Gets the fbc.

        The Facebook click ID value stored in the _fbc browser cookie under your domain.
        See Managing fbc and fbp Parameters for how to get this value
        (https://developers.facebook.com/docs/marketing-api/conversions-api/parameters/customer-information-parameters#fbc),
        or generate this value from a fbclid query parameter.

        :return: The fbc.
        :rtype: str
        """
        return self._fbc

    @fbc.setter
    def fbc(self, fbc):
        """Sets the fbc.

        The Facebook click ID value stored in the _fbc browser cookie under your domain.
        See Managing fbc and fbp Parameters for how to get this value
        (https://developers.facebook.com/docs/marketing-api/conversions-api/parameters/customer-information-parameters#fbc),
        or generate this value from a fbclid query parameter.

        :param fbc: The fbc.
        :type: str
        """

        self._fbc = fbc

    @property
    def fbp(self):
        """Gets the fbp.

        The Facebook browser ID value stored in the _fbp browser cookie under your domain.
        See Managing fbc and fbp Parameters for how to get this value
        (https://developers.facebook.com/docs/marketing-api/conversions-api/parameters/customer-information-parameters#fbc),
        or generate this value from a fbclid query parameter.

        :return: The fbp.
        :rtype: str
        """
        return self._fbp

    @fbp.setter
    def fbp(self, fbp):
        """Sets the fbp.

        The Facebook browser ID value stored in the _fbp browser cookie under your domain.
        See Managing fbc and fbp Parameters for how to get this value
        (https://developers.facebook.com/docs/marketing-api/conversions-api/parameters/customer-information-parameters#fbc),
        or generate this value from a fbclid query parameter.

        :param fbp: The fbp.
        :type: str
        """

        self._fbp = fbp

    @property
    def subscription_id(self):
        """Gets the subscription id.

        The subscription ID for the user in this transaction. This is similar to the order ID for an individual product.

        :return: The subscription id.
        :rtype: str
        """
        return self._subscription_id

    @subscription_id.setter
    def subscription_id(self, subscription_id):
        """Sets the subscription id.

        The subscription ID for the user in this transaction. This is similar to the order ID for an individual product.

        :param subscription_id: The subscription id.
        :type: str
        """

        self._subscription_id = subscription_id

    @property
    def fb_login_id(self):
        """Gets the Facebook login id.

        ID issued by Facebook when a person first logs into an instance of an app. This is also known as App-Scoped ID.

        :return: The Facebook login id.
        :rtype: str
        """
        return self._fb_login_id

    @fb_login_id.setter
    def fb_login_id(self, fb_login_id):
        """Sets the Facebook login id.

        ID issued by Facebook when a person first logs into an instance of an app. This is also known as App-Scoped ID.

        :param fb_login_id: The Facebook login id.
        :type: str
        """

        self._fb_login_id = fb_login_id

    @property
    def lead_id(self):
        """Gets the lead_id.

        Lead ID is associated with a lead generated by Facebook's Lead Ads.

        :return: The lead_id.
        :rtype: str
        """
        return self._lead_id

    @lead_id.setter
    def lead_id(self, lead_id):
        """Sets the lead_id.

        Lead ID is associated with a lead generated by Facebook's Lead Ads.

        :param lead_id: The lead_id.
        :type: str
        """

        self._lead_id = lead_id

    @property
    def f5first(self):
        """Gets the f5first.

        The first 5 characters of a first name.

        :return: f5first.
        :rtype: str
        """
        return self._f5first

    @f5first.setter
    def f5first(self, f5first):
        """Sets the f5first.

        The first 5 characters of a first name.

        :param f5first.
        :type: str
        """

        self._f5first = f5first

    @property
    def f5last(self):
        """Gets the f5last.

        The first 5 characters of a last name.

        :return: f5last.
        :rtype: str
        """
        return self._f5last

    @f5last.setter
    def f5last(self, f5last):
        """Sets the f5last.

        The first 5 characters of a last name.

        :param f5last.
        :type: str
        """

        self._f5last = f5last

    @property
    def fi(self):
        """Gets the fi.

        The first initial.

        :return: fi.
        :rtype: str
        """
        return self._fi

    @fi.setter
    def fi(self, fi):
        """Sets the fi.

        The first initial.

        :param fi.
        :type: str
        """

        self._fi = fi

    @property
    def dobd(self):
        """Gets the dobd.

        The date of birth day.

        :return: dobd.
        :rtype: str
        """
        return self._dobd

    @dobd.setter
    def dobd(self, dobd):
        """Sets the dobd.

        The date of birth day.

        :param dobd.
        :type: str
        """

        self._dobd = dobd

    @property
    def dobm(self):
        """Gets the dobm.

        The date of birth month.

        :return: dobm.
        :rtype: str
        """
        return self._dobm

    @dobm.setter
    def dobm(self, dobm):
        """Sets the dobm.

        The date of birth month.

        :param dobm.
        :type: str
        """

        self._dobm = dobm

    @property
    def doby(self):
        """Gets the doby.

        The date of birth year.

        :return: doby.
        :rtype: str
        """
        return self._doby

    @doby.setter
    def doby(self, doby):
        """Sets the doby.

        The date of birth year.

        :param doby.
        :type: str
        """

        self._doby = doby

    @property
    def madid(self):
        return self._madid

    @madid.setter
    def madid(self, madid):
        self._madid = madid

    @property
    def anon_id(self):
        return self._anon_id

    @anon_id.setter
    def anon_id(self, anon_id):
        self._anon_id = anon_id

    @property
    def ctwa_clid(self):
        return self._ctwa_clid

    @ctwa_clid.setter
    def ctwa_clid(self, ctwa_clid):
        self._ctwa_clid = ctwa_clid

    @property
    def page_id(self):
        return self._page_id

    @page_id.setter
    def page_id(self, page_id):
        self._page_id = page_id

    def normalize(self):
        normalized_payload = {'em': self.__normalize_list('em', self.emails),
                              'ph': self.__normalize_list('ph', self.phones),
                              'db': self.__normalize_list('db', self.dates_of_birth),
                              'ln': self.__normalize_list('ln', self.last_names),
                              'fn': self.__normalize_list('fn', self.first_names),
                              'ct': self.__normalize_list('ct', self.cities),
                              'st': self.__normalize_list('st', self.states),
                              'zp': self.__normalize_list('zp', self.zip_codes),
                              'country': self.__normalize_list('country', self.country_codes),
                              'external_id': self.__dedup_list(self.external_ids),
                              'client_ip_address': self.client_ip_address,
                              'client_user_agent': self.client_user_agent,
                              'fbc': self.fbc,
                              'fbp': self.fbp,
                              'subscription_id': self.subscription_id,
                              'fb_login_id': self.fb_login_id,
                              'lead_id': self.lead_id,
                              'f5first': Normalize.normalize_field('f5first', self.f5first),
                              'f5last': Normalize.normalize_field('f5last', self.f5last),
                              'fi': Normalize.normalize_field('fi', self.fi),
                              'dobd': Normalize.normalize_field('dobd', self.dobd),
                              'dobm': Normalize.normalize_field('dobm', self.dobm),
                              'doby': Normalize.normalize_field('doby', self.doby),
                              'madid': self.madid,
                              'anon_id': self.anon_id,
                              'ctwa_clid': self.ctwa_clid,
                              'page_id': self.page_id,
                              }
        if self.genders:
            normalized_payload['ge'] = self.__normalize_list('ge', list(map(lambda g: g.value, self.genders)))

        normalized_payload = {k: v for k, v in normalized_payload.items() if v is not None}
        return normalized_payload

    def __normalize_list(self, field_name, value_list):
        """Dedup, hash and normalize the given list.

        :type: field_name: str
        :type value_list: list[str]
        :rtype: dict
        """
        if field_name is None or not value_list:
            return None
        normalized_list = list(map(lambda val: Normalize.normalize_field(field_name, val), value_list))
        return self.__dedup_list(normalized_list)

    def __dedup_list(self, value_list):
        """Dedup the given list.

        :type value_list: list[str]
        :rtype: dict
        """
        if not value_list:
            return None
        return list(set(value_list))

    def to_dict(self):
        """Returns the model properties as a dict"""
        result = {}

        for attr, _ in six.iteritems(self.param_types):
            value = getattr(self, attr)
            if isinstance(value, list):
                result[attr] = list(map(
                    lambda x: x.to_dict() if hasattr(x, "to_dict") else x,
                    value
                ))
            elif hasattr(value, "to_dict"):
                result[attr] = value.to_dict()
            elif isinstance(value, dict):
                result[attr] = dict(map(
                    lambda item: (item[0], item[1].to_dict())
                    if hasattr(item[1], "to_dict") else item,
                    value.items()
                ))
            else:
                result[attr] = value
        if issubclass(UserData, dict):
            for key, value in self.items():
                result[key] = value

        return result

    def to_str(self):
        """Returns the string representation of the model"""
        return pprint.pformat(self.to_dict())

    def __repr__(self):
        """For `print` and `pprint`"""
        return self.to_str()

    def __eq__(self, other):
        """Returns true if both objects are equal"""
        if not isinstance(other, UserData):
            return False

        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        """Returns true if both objects are not equal"""
        return not self == other
