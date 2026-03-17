from unittest import TestCase

from webauthn.helpers.snake_case_to_camel_case import snake_case_to_camel_case


class TestWebAuthnSnakeCaseToCamelCase(TestCase):
    def test_converts_snake_case_to_camel_case(self) -> None:
        output = snake_case_to_camel_case("snake_case")

        assert output == "snakeCase"

    def test_converts_client_data_json(self) -> None:
        output = snake_case_to_camel_case("client_data_json")

        assert output == "clientDataJSON"
