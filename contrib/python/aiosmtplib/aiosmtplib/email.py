"""
Email message and address formatting/parsing functions.
"""

import copy
import email.charset
import email.generator
import email.header
import email.headerregistry
import email.message
import email.policy
import email.utils
import io
import re
from collections.abc import Iterable
from typing import cast


__all__ = (
    "extract_recipients",
    "extract_sender",
    "flatten_message",
    "parse_address",
    "quote_address",
)


SPECIALS_REGEX = re.compile(r'[][\\()<>@,:;".]')
ESCAPES_REGEX = re.compile(r'[\\"]')
UTF8_CHARSET = email.charset.Charset("utf-8")


def parse_address(address: str) -> str:
    """
    Parse an email address, falling back to the raw string given.
    """
    _, parsed_address = email.utils.parseaddr(address)

    return parsed_address or address.strip()


def quote_address(address: str) -> str:
    """
    Quote a subset of the email addresses defined by RFC 821.
    """
    parsed_address = parse_address(address)
    return f"<{parsed_address}>"


def flatten_message(
    message: email.message.EmailMessage | email.message.Message,
    /,
    *,
    utf8: bool = False,
    cte_type: str = "8bit",
) -> bytes:
    # Make a local copy so we can delete the bcc headers.
    message_copy = copy.copy(message)
    del message_copy["Bcc"]
    del message_copy["Resent-Bcc"]

    with io.BytesIO() as messageio:
        if isinstance(message_copy, email.message.EmailMessage):
            policy = email.policy.SMTPUTF8 if utf8 else email.policy.SMTP

            if cte_type != "8bit":
                policy = policy.clone(cte_type=cte_type)

            generator = email.generator.BytesGenerator(messageio, policy=policy)
            generator.flatten(message_copy)
        else:
            # Old message class, Compat32 policy. Compat32 cannot use UTF8
            # Mypy can't handle message unions, so just use different vars
            compat_policy = email.policy.compat32
            if cte_type != "8bit":
                compat_policy = compat_policy.clone(cte_type=cte_type)

            compat_generator = email.generator.BytesGenerator(
                messageio, policy=compat_policy
            )
            compat_generator.flatten(message_copy)

        flat_message = messageio.getvalue()

    return flat_message


def extract_addresses(
    header: str | email.headerregistry.AddressHeader | email.header.Header,
    /,
) -> list[str]:
    """
    Convert address headers into raw email addresses, suitable for use in
    low level SMTP commands.
    """
    addresses: list[str] = []
    if isinstance(header, email.headerregistry.AddressHeader):
        # If the object has been assigned an iterable, it's possible to get a string here.
        header_addresses = cast(
            Iterable[str | email.headerregistry.Address], header.addresses
        )
        for address in header_addresses:
            if isinstance(address, email.headerregistry.Address):
                addresses.append(address.addr_spec)
            else:
                addresses.append(parse_address(address))
    elif isinstance(header, email.header.Header):
        for address_bytes, charset in email.header.decode_header(header):
            address_str = str(address_bytes, encoding=charset or "ascii")
            addresses.append(parse_address(address_str))
    else:
        addresses.extend(addr for _, addr in email.utils.getaddresses([header]))

    return addresses


def extract_sender(
    message: email.message.EmailMessage | email.message.Message,
    /,
) -> str | None:
    """
    Extract the sender from the message object given.
    """
    resent_dates = message.get_all("Resent-Date")

    if resent_dates is not None and len(resent_dates) > 1:
        raise ValueError("Message has more than one 'Resent-' header block")
    elif resent_dates:
        sender_header_name = "Resent-Sender"
        from_header_name = "Resent-From"
    else:
        sender_header_name = "Sender"
        from_header_name = "From"

    # Prefer the sender field per RFC 2822:3.6.2.
    if sender_header_name in message:
        sender_header = message[sender_header_name]
    else:
        sender_header = message[from_header_name]

    if sender_header is None:
        return None

    return extract_addresses(sender_header)[0]


def extract_recipients(
    message: email.message.EmailMessage | email.message.Message,
    /,
) -> list[str]:
    """
    Extract the recipients from the message object given.
    """
    recipients: list[str] = []

    resent_dates = message.get_all("Resent-Date")

    if resent_dates is not None and len(resent_dates) > 1:
        raise ValueError("Message has more than one 'Resent-' header block")
    elif resent_dates:
        recipient_headers = ("Resent-To", "Resent-Cc", "Resent-Bcc")
    else:
        recipient_headers = ("To", "Cc", "Bcc")

    for header in recipient_headers:
        for recipient in message.get_all(header, failobj=[]):
            recipients.extend(extract_addresses(recipient))

    return recipients
