"""codspeed benchmarks for parsing query strings and form bodies."""

import base64
import json
from urllib.parse import urlencode

import pytest

try:
    from pytest_codspeed import BenchmarkFixture
except ImportError:  # pragma: no branch  # only hit in cibuildwheel
    pytestmark = pytest.mark.skip("pytest-codspeed needs to be installed")

from yarl import query_to_pairs

# Stable stand-ins for random tokens; standard base64 exercises %2B, %2F, %3D
TOKEN = base64.b64encode(bytes(range(48))).decode()
URLSAFE_TOKEN = base64.urlsafe_b64encode(bytes(range(96))).decode().rstrip("=")

# Bodies below are encoded with urlencode(), which matches what browsers and
# HTTP client libraries send for application/x-www-form-urlencoded.

LOGIN_FORM = urlencode(
    {
        "csrfmiddlewaretoken": TOKEN,
        "username": "jane.doe+test@example.com",
        "password": "c0rrect horse!battery$staple",
        "remember_me": "on",
        "next": "/account/settings/?tab=security&ref=login",
    }
)

OAUTH_TOKEN_REQUEST = urlencode(
    {
        "grant_type": "authorization_code",
        "code": URLSAFE_TOKEN,
        "redirect_uri": "https://app.example.com/oauth/callback?provider=example",
        "client_id": "0oa1b2c3d4e5f6g7h8i9",
        "client_secret": TOKEN,
        "code_verifier": URLSAFE_TOKEN,
        "scope": "openid profile email offline_access",
    }
)

SMS_WEBHOOK = urlencode(
    {
        "ToCountry": "US",
        "ToState": "CA",
        "SmsMessageSid": "SM" + "x" * 32,
        "NumMedia": "0",
        "ToCity": "SAN FRANCISCO",
        "FromZip": "94105",
        "SmsSid": "SM" + "x" * 32,
        "FromState": "CA",
        "SmsStatus": "received",
        "FromCity": "SAN FRANCISCO",
        "Body": "Running 10 min late, sorry! Can you order me a coffee? ☕",
        "FromCountry": "US",
        "To": "+15558675309",
        "MessagingServiceSid": "MG" + "x" * 32,
        "ToZip": "94107",
        "NumSegments": "1",
        "MessageSid": "SM" + "x" * 32,
        "AccountSid": "AC" + "x" * 32,
        "From": "+15551234567",
        "ApiVersion": "2010-04-01",
    }
)

CHECKOUT_FORM = urlencode(
    {
        "authenticity_token": TOKEN,
        "order[email]": "jane.doe@example.com",
        "order[shipping_address][first_name]": "Jane",
        "order[shipping_address][last_name]": "O'Connor-Smith",
        "order[shipping_address][address1]": "1234 Market Street, Suite 500",
        "order[shipping_address][address2]": "Attn: Receiving Dock #3",
        "order[shipping_address][city]": "San Francisco",
        "order[shipping_address][province]": "CA",
        "order[shipping_address][zip]": "94103",
        "order[shipping_address][country]": "United States",
        "order[shipping_address][phone]": "+1 (555) 123-4567",
        "order[billing_same_as_shipping]": "1",
        "order[shipping_method]": "ups-ground-3-5-business-days",
        "order[note]": "Please leave the package with the front desk, thanks!",
        "order[discount_code]": "SPRING-25%OFF",
        "order[accepts_marketing]": "0",
        "commit": "Continue to payment",
    }
)

# Rails and PHP style nested arrays repeat the same key for every entry
NESTED_ORDER_ITEMS = urlencode(
    [("utf8", "✓"), ("authenticity_token", TOKEN)]
    + [
        pair
        for i in range(50)
        for pair in (
            ("order[line_items][][product_id]", str(1000 + i)),
            ("order[line_items][][variant_id]", str(50000 + i * 7)),
            ("order[line_items][][quantity]", str(i % 5 + 1)),
            ("order[line_items][][properties][gift_message]", f"Happy birthday #{i}!"),
        )
    ]
)

MULTILINGUAL_CONTACT_FORM = urlencode(
    {
        "name": "Zoë Kowalczyk 山田太郎",
        "email": "zoe@example.org",
        "subject": "Question about my order — #10482",
        "message": (
            "Hi there,\r\n\r\n"
            "I ordered the “Deluxe” kit last week and it hasn’t arrived.\r\n"
            "注文番号は10482です。"
            "よろしくお願いします。\r\n"
            "Здравствуйте, "
            "спасибо! \U0001f64f\r\n\r\n"
            "Thanks,\r\nZoë"
        )
        * 8,
        "consent": "yes",
    }
)

SLACK_INTERACTIVE_PAYLOAD = urlencode(
    {
        "payload": json.dumps(
            {
                "type": "block_actions",
                "user": {"id": "U0123ABCD", "username": "jane", "team_id": "T0123"},
                "api_app_id": "A0123ABCD",
                "token": URLSAFE_TOKEN[:24],
                "container": {
                    "type": "message",
                    "message_ts": "1700000000.000100",
                    "channel_id": "C0123ABCD",
                    "is_ephemeral": False,
                },
                "trigger_id": "1234567890.1234567890.abcdef0123456789",
                "team": {"id": "T0123", "domain": "example-workspace"},
                "channel": {"id": "C0123ABCD", "name": "deploys"},
                "response_url": "https://hooks.slack.com/actions/T0123/1234/abcdef",
                "actions": [
                    {
                        "action_id": f"approve_{i}",
                        "block_id": f"deploy_block_{i}",
                        "text": {"type": "plain_text", "text": "Approve ✅"},
                        "value": f'{{"deploy_id": {i}, "env": "production"}}',
                        "type": "button",
                        "action_ts": "1700000001.000200",
                    }
                    for i in range(10)
                ],
            }
        )
    }
)

WEBHOOK_PUSH_PAYLOAD = urlencode(
    {
        "payload": json.dumps(
            {
                "ref": "refs/heads/main",
                "before": "0" * 40,
                "after": "f" * 40,
                "repository": {
                    "id": 123456789,
                    "full_name": "example-org/example-repo",
                    "html_url": "https://github.com/example-org/example-repo",
                    "description": "An example repository, with ünicode & symbols",
                },
                "pusher": {"name": "jane", "email": "jane@example.com"},
                "commits": [
                    {
                        "id": f"{i:040x}",
                        "message": (
                            f"Fix issue #{i}: handle edge case in parser\n\n"
                            "Signed-off-by: Jane Doe <jane@example.com>"
                        ),
                        "timestamp": "2024-01-01T12:00:00+00:00",
                        "url": f"https://github.com/example-org/example-repo/commit/{i:040x}",
                        "author": {"name": "Jane Doe", "email": "jane@example.com"},
                        "added": [f"src/module_{i}/new file.py"],
                        "removed": [],
                        "modified": ["README.md", f"src/module_{i}/__init__.py"],
                    }
                    for i in range(20)
                ],
            }
        )
    }
)

REAL_WORLD_BODIES = {
    "login_form": LOGIN_FORM,
    "oauth_token_request": OAUTH_TOKEN_REQUEST,
    "sms_webhook": SMS_WEBHOOK,
    "checkout_form": CHECKOUT_FORM,
    "nested_order_items": NESTED_ORDER_ITEMS,
    "multilingual_contact_form": MULTILINGUAL_CONTACT_FORM,
    "slack_interactive_payload": SLACK_INTERACTIVE_PAYLOAD,
    "webhook_push_payload": WEBHOOK_PUSH_PAYLOAD,
}

SYNTHETIC_QUERY_STRINGS = {
    "many_fields": "&".join(f"field{i}=value{i}" for i in range(1000)),
    "many_fields_pct": "&".join(f"f%C3%A9{i}=v+%E2%82%AC+{i}" for i in range(1000)),
    "long_value": "text=" + "lorem+ipsum+dolor+sit+amet%2C+" * 512,
    "repeated_key": "x&" * 1000,
}


@pytest.mark.parametrize(
    "body", REAL_WORLD_BODIES.values(), ids=REAL_WORLD_BODIES.keys()
)
def test_query_to_pairs_real_world(benchmark: "BenchmarkFixture", body: str) -> None:
    @benchmark
    def _run() -> None:
        for _ in range(10):
            query_to_pairs(body)


@pytest.mark.parametrize(
    "query_string",
    SYNTHETIC_QUERY_STRINGS.values(),
    ids=SYNTHETIC_QUERY_STRINGS.keys(),
)
def test_query_to_pairs_synthetic(
    benchmark: "BenchmarkFixture", query_string: str
) -> None:
    @benchmark
    def _run() -> None:
        for _ in range(10):
            query_to_pairs(query_string)
