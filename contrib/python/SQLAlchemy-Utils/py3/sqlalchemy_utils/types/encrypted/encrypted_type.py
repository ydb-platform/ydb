import base64
import datetime
import json
import os
import warnings

from sqlalchemy.types import LargeBinary, String, TypeDecorator

from sqlalchemy_utils.exceptions import ImproperlyConfigured
from sqlalchemy_utils.types.encrypted.padding import PADDING_MECHANISM
from sqlalchemy_utils.types.json import JSONType
from sqlalchemy_utils.types.scalar_coercible import ScalarCoercible

cryptography = None
try:
    import cryptography
    from cryptography.exceptions import InvalidTag
    from cryptography.fernet import Fernet
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.ciphers import (
        algorithms,
        Cipher,
        modes
    )
except ImportError:
    pass

dateutil = None
try:
    import dateutil
    from dateutil.parser import parse as datetime_parse
except ImportError:
    pass


class InvalidCiphertextError(Exception):
    pass


class EncryptionDecryptionBaseEngine:
    """A base encryption and decryption engine.

    This class must be sub-classed in order to create
    new engines.
    """

    def _update_key(self, key):
        if isinstance(key, str):
            key = key.encode()
        digest = hashes.Hash(hashes.SHA256(), backend=default_backend())
        digest.update(key)
        engine_key = digest.finalize()

        self._initialize_engine(engine_key)

    def encrypt(self, value):
        raise NotImplementedError('Subclasses must implement this!')

    def decrypt(self, value):
        raise NotImplementedError('Subclasses must implement this!')


class AesEngine(EncryptionDecryptionBaseEngine):
    """Provide AES encryption and decryption methods.

    You may also consider using the AesGcmEngine instead -- that may be
    a better fit for some cases.

    You should NOT use the AesGcmEngine if you want to be able to search
    for a row based on the value of an encrypted column. Use AesEngine
    instead, since that allows you to perform such searches.

    If you don't need to search by the value of an encypted column, the
    AesGcmEngine provides better security.
    """

    BLOCK_SIZE = 16

    def _initialize_engine(self, parent_class_key):
        self.secret_key = parent_class_key
        self.iv = self.secret_key[:16]
        self.cipher = Cipher(
            algorithms.AES(self.secret_key),
            modes.CBC(self.iv),
            backend=default_backend()
        )

    def _set_padding_mechanism(self, padding_mechanism=None):
        """Set the padding mechanism."""
        if isinstance(padding_mechanism, str):
            if padding_mechanism not in PADDING_MECHANISM.keys():
                raise ImproperlyConfigured(
                    "There is not padding mechanism with name {}".format(
                        padding_mechanism
                    )
                )

        if padding_mechanism is None:
            padding_mechanism = 'naive'

        padding_class = PADDING_MECHANISM[padding_mechanism]
        self.padding_engine = padding_class(self.BLOCK_SIZE)

    def encrypt(self, value):
        if not isinstance(value, str):
            value = repr(value)
        if isinstance(value, str):
            value = str(value)
        value = value.encode()
        value = self.padding_engine.pad(value)
        encryptor = self.cipher.encryptor()
        encrypted = encryptor.update(value) + encryptor.finalize()
        encrypted = base64.b64encode(encrypted)
        return encrypted.decode('utf-8')

    def decrypt(self, value):
        if isinstance(value, str):
            value = str(value)
        decryptor = self.cipher.decryptor()
        decrypted = base64.b64decode(value)
        decrypted = decryptor.update(decrypted) + decryptor.finalize()
        decrypted = self.padding_engine.unpad(decrypted)
        if not isinstance(decrypted, str):
            try:
                decrypted = decrypted.decode('utf-8')
            except UnicodeDecodeError:
                raise ValueError('Invalid decryption key')
        return decrypted


class AesGcmEngine(EncryptionDecryptionBaseEngine):
    """Provide AES/GCM encryption and decryption methods.

    You may also consider using the AesEngine instead -- that may be
    a better fit for some cases.

    You should NOT use this AesGcmEngine if you want to be able to search
    for a row based on the value of an encrypted column. Use AesEngine
    instead, since that allows you to perform such searches.

    If you don't need to search by the value of an encypted column, the
    AesGcmEngine provides better security.
    """

    BLOCK_SIZE = 16
    IV_BYTES_NEEDED = 12
    TAG_SIZE_BYTES = BLOCK_SIZE

    def _initialize_engine(self, parent_class_key):
        self.secret_key = parent_class_key

    def encrypt(self, value):
        if not isinstance(value, str):
            value = repr(value)
        if isinstance(value, str):
            value = str(value)
        value = value.encode()
        iv = os.urandom(self.IV_BYTES_NEEDED)
        cipher = Cipher(
            algorithms.AES(self.secret_key),
            modes.GCM(iv),
            backend=default_backend()
        )
        encryptor = cipher.encryptor()
        encrypted = encryptor.update(value) + encryptor.finalize()
        assert len(encryptor.tag) == self.TAG_SIZE_BYTES
        encrypted = base64.b64encode(iv + encryptor.tag + encrypted)
        return encrypted.decode('utf-8')

    def decrypt(self, value):
        if isinstance(value, str):
            value = str(value)
        decrypted = base64.b64decode(value)
        if len(decrypted) < self.IV_BYTES_NEEDED + self.TAG_SIZE_BYTES:
            raise InvalidCiphertextError()
        iv = decrypted[:self.IV_BYTES_NEEDED]
        tag = decrypted[self.IV_BYTES_NEEDED:
                        self.IV_BYTES_NEEDED + self.TAG_SIZE_BYTES]
        decrypted = decrypted[self.IV_BYTES_NEEDED + self.TAG_SIZE_BYTES:]
        cipher = Cipher(
            algorithms.AES(self.secret_key),
            modes.GCM(iv, tag),
            backend=default_backend()
        )
        decryptor = cipher.decryptor()
        try:
            decrypted = decryptor.update(decrypted) + decryptor.finalize()
        except InvalidTag:
            raise InvalidCiphertextError()
        if not isinstance(decrypted, str):
            try:
                decrypted = decrypted.decode('utf-8')
            except UnicodeDecodeError:
                raise InvalidCiphertextError()
        return decrypted


class FernetEngine(EncryptionDecryptionBaseEngine):
    """Provide Fernet encryption and decryption methods."""

    def _initialize_engine(self, parent_class_key):
        self.secret_key = base64.urlsafe_b64encode(parent_class_key)
        self.fernet = Fernet(self.secret_key)

    def encrypt(self, value):
        if not isinstance(value, str):
            value = repr(value)
        if isinstance(value, str):
            value = str(value)
        value = value.encode()
        encrypted = self.fernet.encrypt(value)
        return encrypted.decode('utf-8')

    def decrypt(self, value):
        if isinstance(value, str):
            value = str(value)
        decrypted = self.fernet.decrypt(value.encode())
        if not isinstance(decrypted, str):
            decrypted = decrypted.decode('utf-8')
        return decrypted


class StringEncryptedType(TypeDecorator, ScalarCoercible):
    """
    StringEncryptedType provides a way to encrypt and decrypt values,
    to and from databases, that their type is a basic SQLAlchemy type.
    For example Unicode, String or even Boolean.
    On the way in, the value is encrypted and on the way out the stored value
    is decrypted.

    StringEncryptedType needs Cryptography_ library in order to work.

    When declaring a column which will be of type StringEncryptedType
    it is better to be as precise as possible and follow the pattern
    below.

    .. _Cryptography: https://cryptography.io/en/latest/

    ::


        a_column = sa.Column(StringEncryptedType(sa.Unicode,
                                           secret_key,
                                           FernetEngine))

        another_column = sa.Column(StringEncryptedType(sa.Unicode,
                                           secret_key,
                                           AesEngine,
                                           'pkcs5'))


    A more complete example is given below.

    ::


        import sqlalchemy as sa
        from sqlalchemy import create_engine
        try:
            from sqlalchemy.orm import declarative_base
        except ImportError:
            # sqlalchemy 1.3
            from sqlalchemy.ext.declarative import declarative_base
        from sqlalchemy.orm import sessionmaker

        from sqlalchemy_utils import StringEncryptedType
        from sqlalchemy_utils.types.encrypted.encrypted_type import AesEngine

        secret_key = 'secretkey1234'
        # setup
        engine = create_engine('sqlite:///:memory:')
        connection = engine.connect()
        Base = declarative_base()


        class User(Base):
            __tablename__ = "user"
            id = sa.Column(sa.Integer, primary_key=True)
            username = sa.Column(StringEncryptedType(sa.Unicode,
                                               secret_key,
                                               AesEngine,
                                               'pkcs5'))
            access_token = sa.Column(StringEncryptedType(sa.String,
                                                   secret_key,
                                                   AesEngine,
                                                   'pkcs5'))
            is_active = sa.Column(StringEncryptedType(sa.Boolean,
                                                secret_key,
                                                AesEngine,
                                                'zeroes'))
            number_of_accounts = sa.Column(StringEncryptedType(sa.Integer,
                                                         secret_key,
                                                         AesEngine,
                                                         'oneandzeroes'))


        sa.orm.configure_mappers()
        Base.metadata.create_all(connection)

        # create a configured "Session" class
        Session = sessionmaker(bind=connection)

        # create a Session
        session = Session()

        # example
        user_name = 'secret_user'
        test_token = 'atesttoken'
        active = True
        num_of_accounts = 2

        user = User(username=user_name, access_token=test_token,
                    is_active=active, number_of_accounts=num_of_accounts)
        session.add(user)
        session.commit()

        user_id = user.id

        session.expunge_all()

        user_instance = session.query(User).get(user_id)

        print('id: {}'.format(user_instance.id))
        print('username: {}'.format(user_instance.username))
        print('token: {}'.format(user_instance.access_token))
        print('active: {}'.format(user_instance.is_active))
        print('accounts: {}'.format(user_instance.number_of_accounts))

        # teardown
        session.close_all()
        Base.metadata.drop_all(connection)
        connection.close()
        engine.dispose()

    The key parameter accepts a callable to allow for the key to change
    per-row instead of being fixed for the whole table.

    ::


        def get_key():
            return 'dynamic-key'

        class User(Base):
            __tablename__ = 'user'
            id = sa.Column(sa.Integer, primary_key=True)
            username = sa.Column(StringEncryptedType(
                sa.Unicode, get_key))

    """
    impl = String
    cache_ok = True

    def __init__(
        self,
        type_in=None,
        key=None,
        engine=None,
        padding=None,
        **kwargs
    ):
        """Initialization."""
        if not cryptography:
            raise ImproperlyConfigured(
                "'cryptography' is required to use StringEncryptedType"
            )
        super().__init__(**kwargs)
        # set the underlying type
        if type_in is None:
            type_in = String()
        elif isinstance(type_in, type):
            type_in = type_in()
        self.underlying_type = type_in
        self._key = key
        if not engine:
            engine = AesEngine
        self.engine = engine()
        if isinstance(self.engine, AesEngine):
            self.engine._set_padding_mechanism(padding)

    @property
    def key(self):
        return self._key

    @key.setter
    def key(self, value):
        self._key = value

    def _update_key(self):
        key = self._key() if callable(self._key) else self._key
        self.engine._update_key(key)

    def process_bind_param(self, value, dialect):
        """Encrypt a value on the way in."""
        if value is not None:
            self._update_key()

            try:
                value = self.underlying_type.process_bind_param(
                    value, dialect
                )

            except AttributeError:
                # Doesn't have 'process_bind_param'

                # Handle 'boolean' and 'dates'
                type_ = self.underlying_type.python_type
                if issubclass(type_, bool):
                    value = 'true' if value else 'false'

                elif issubclass(type_, (datetime.date, datetime.time)):
                    value = value.isoformat()

                elif issubclass(type_, JSONType):
                    value = json.dumps(value)

            return self.engine.encrypt(value)

    def process_result_value(self, value, dialect):
        """Decrypt value on the way out."""
        if value is not None:
            self._update_key()
            decrypted_value = self.engine.decrypt(value)

            try:
                return self.underlying_type.process_result_value(
                    decrypted_value, dialect
                )

            except AttributeError:
                # Doesn't have 'process_result_value'

                # Handle 'boolean' and 'dates'
                type_ = self.underlying_type.python_type
                date_types = [datetime.datetime, datetime.time, datetime.date]

                if issubclass(type_, bool):
                    return decrypted_value == 'true'

                elif type_ in date_types:
                    return DatetimeHandler.process_value(
                        decrypted_value, type_
                    )

                elif issubclass(type_, JSONType):
                    return json.loads(decrypted_value)

                # Handle all others
                return self.underlying_type.python_type(decrypted_value)

    def _coerce(self, value):
        if isinstance(self.underlying_type, ScalarCoercible):
            return self.underlying_type._coerce(value)

        return value


class EncryptedType(StringEncryptedType):
    """
    The 'EncryptedType' class will change implementation from
    'LargeBinary' to 'String' in a future version. Use
    'StringEncryptedType' to use the 'String' implementation.
    """
    impl = LargeBinary

    def __init__(self, *args, **kwargs):
        warnings.warn(
            "The 'EncryptedType' class will change implementation from "
            "'LargeBinary' to 'String' in a future version. Use "
            "'StringEncryptedType' to use the 'String' implementation.",
            DeprecationWarning, stacklevel=2)
        super().__init__(*args, **kwargs)

    def process_bind_param(self, value, dialect):
        value = super().process_bind_param(value=value, dialect=dialect)
        if isinstance(value, str):
            value = value.encode()
        return value

    def process_result_value(self, value, dialect):
        if isinstance(value, bytes):
            value = value.decode()
            value = super().process_result_value(value=value, dialect=dialect)
        return value


class DatetimeHandler:
    """
    DatetimeHandler is responsible for parsing strings and
    returning the appropriate date, datetime or time objects.
    """

    @classmethod
    def process_value(cls, value, python_type):
        """
        process_value returns a datetime, date
        or time object according to a given string
        value and a python type.
        """
        if not dateutil:
            raise ImproperlyConfigured(
                "'python-dateutil' is required to process datetimes"
            )

        return_value = datetime_parse(value)

        if issubclass(python_type, datetime.datetime):
            return return_value
        elif issubclass(python_type, datetime.time):
            return return_value.time()
        elif issubclass(python_type, datetime.date):
            return return_value.date()
