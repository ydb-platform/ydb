# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.

# This source code is licensed under the license found in the
# LICENSE file in the root directory of this source tree.

from facebook_business.exceptions import FacebookBadObjectError

import hashlib
import six
import re

class CustomAudienceMixin:

    class Schema(object):
        uid = 'UID'
        email_hash = 'EMAIL_SHA256'
        phone_hash = 'PHONE_SHA256'
        mobile_advertiser_id = 'MOBILE_ADVERTISER_ID'

        class MultiKeySchema(object):
            extern_id = 'EXTERN_ID'
            email = 'EMAIL'
            phone = 'PHONE'
            gen = 'GEN'
            doby = 'DOBY'
            dobm = 'DOBM'
            dobd = 'DOBD'
            ln = 'LN'
            fn = 'FN'
            fi = 'FI'
            ct = 'CT'
            st = 'ST'
            zip = 'ZIP'
            madid = 'MADID'
            country = 'COUNTRY'
            appuid = 'APPUID'

    @classmethod
    def format_params(cls,
                      schema,
                      users,
                      is_raw=False,
                      app_ids=None,
                      pre_hashed=None,
                      session=None):
        hashed_users = []
        if schema in (cls.Schema.phone_hash,
                      cls.Schema.email_hash,
                      cls.Schema.mobile_advertiser_id):
            for user in users:
                if schema == cls.Schema.email_hash:
                    user = user.strip(" \t\r\n\0\x0B.").lower()
                if isinstance(user, six.text_type) and not(pre_hashed) and schema != cls.Schema.mobile_advertiser_id:
                    user = user.encode('utf8')  # required for hashlib
                # for mobile_advertiser_id, don't hash it
                if pre_hashed or schema == cls.Schema.mobile_advertiser_id:
                    hashed_users.append(user)
                else:
                    hashed_users.append(hashlib.sha256(user).hexdigest())
        elif isinstance(schema, list):
            # SDK will support only single PII
            if not is_raw:
                raise FacebookBadObjectError(
                    "Please send single PIIs i.e. is_raw should be true. " +
                    "The combining of the keys will be done internally.",
                )
            # users is array of array
            for user in users:
                if len(schema) != len(user):
                    raise FacebookBadObjectError(
                        "Number of keys in each list in the data should " +
                        "match the number of keys specified in scheme",
                    )
                    break

                # If the keys are already hashed then send as it is
                if pre_hashed:
                    hashed_users.append(user)
                else:
                    counter = 0
                    hashed_user = []
                    for key in user:
                        key = key.strip(" \t\r\n\0\x0B.").lower()
                        key = cls.normalize_key(schema[counter],
                                                           str(key))
                        if schema[counter] != \
                                cls.Schema.MultiKeySchema.extern_id:
                            if isinstance(key, six.text_type):
                                key = key.encode('utf8')
                            key = hashlib.sha256(key).hexdigest()
                        counter = counter + 1
                        hashed_user.append(key)
                    hashed_users.append(hashed_user)

        payload = {
            'schema': schema,
            'is_raw': is_raw,
            'data': hashed_users or users,
        }

        if schema == cls.Schema.uid:
            if not app_ids:
                raise FacebookBadObjectError(
                    "Custom Audiences with type " + cls.Schema.uid +
                    "require at least one app_id",
                )

        if app_ids:
            payload['app_ids'] = app_ids

        params = {
            'payload': payload,
        }
        if session:
            params['session'] = session
        return params

    @classmethod
    def normalize_key(cls, key_name, key_value=None):
        """
            Normalize the value based on the key
        """
        if key_value is None:
            return key_value

        if(key_name == cls.Schema.MultiKeySchema.extern_id or
           key_name == cls.Schema.MultiKeySchema.email or
           key_name == cls.Schema.MultiKeySchema.madid or
           key_name == cls.Schema.MultiKeySchema.appuid):
            return key_value

        if(key_name == cls.Schema.MultiKeySchema.phone):
            key_value = re.sub(r'[^0-9]', '', key_value)
            return key_value

        if(key_name == cls.Schema.MultiKeySchema.gen):
            key_value = key_value.strip()[:1]
            return key_value

        if(key_name == cls.Schema.MultiKeySchema.doby):
            key_value = re.sub(r'[^0-9]', '', key_value)
            return key_value

        if(key_name == cls.Schema.MultiKeySchema.dobm or
           key_name == cls.Schema.MultiKeySchema.dobd):

            key_value = re.sub(r'[^0-9]', '', key_value)
            if len(key_value) == 1:
                key_value = '0' + key_value
            return key_value

        if(key_name == cls.Schema.MultiKeySchema.ln or
           key_name == cls.Schema.MultiKeySchema.fn or
           key_name == cls.Schema.MultiKeySchema.ct or
           key_name == cls.Schema.MultiKeySchema.fi or
           key_name == cls.Schema.MultiKeySchema.st):
            key_value = re.sub(r'[^a-zA-Z]', '', key_value)
            return key_value

        if(key_name == cls.Schema.MultiKeySchema.zip):
            key_value = re.split('-', key_value)[0]
            return key_value

        if(key_name == cls.Schema.MultiKeySchema.country):
            key_value = re.sub(r'[^a-zA-Z]', '', key_value)[:2]
            return key_value

    def add_users(self,
                  schema,
                  users,
                  is_raw=False,
                  app_ids=None,
                  pre_hashed=None,
                  session=None):
        """Adds users to this CustomAudience.

        Args:
            schema: A CustomAudience.Schema value specifying the type of values
                in the users list.
            users: A list of identities respecting the schema specified.

        Returns:
            The FacebookResponse object.
        """
        return self.get_api_assured().call(
            'POST',
            (self.get_id_assured(), 'users'),
            params=self.format_params(
                schema,
                users,
                is_raw,
                app_ids,
                pre_hashed,
                session,
            ),
        )

    def remove_users(self,
                     schema,
                     users,
                     is_raw=False,
                     app_ids=None,
                     pre_hashed=None,
                     session=None):
        """Deletes users from this CustomAudience.

        Args:
            schema: A CustomAudience.Schema value specifying the type of values
                in the users list.
            users: A list of identities respecting the schema specified.

        Returns:
            The FacebookResponse object.
        """
        return self.get_api_assured().call(
            'DELETE',
            (self.get_id_assured(), 'users'),
            params=self.format_params(
                schema,
                users,
                is_raw,
                app_ids,
                pre_hashed,
                session,
            ),
        )

    def share_audience(self, account_ids):
        """Shares this CustomAudience with the specified account_ids.

        Args:
            account_ids: A list of account ids.

        Returns:
            The FacebookResponse object.
        """
        return self.get_api_assured().call(
            'POST',
            (self.get_id_assured(), 'adaccounts'),
            params={'adaccounts': account_ids},
        )

    def unshare_audience(self, account_ids):
        """Unshares this CustomAudience with the specified account_ids.

        Args:
            account_ids: A list of account ids.

        Returns:
            The FacebookResponse object.
        """
        return self.get_api_assured().call(
            'DELETE',
            (self.get_id_assured(), 'adaccounts'),
            params={'adaccounts': account_ids},
        )
