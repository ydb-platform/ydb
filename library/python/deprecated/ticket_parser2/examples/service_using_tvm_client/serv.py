import requests
import ticket_parser2 as tp2


class SomeService:
    def __init__(self, **kwargs):
        self._client = tp2.TvmClient(
            tp2.TvmApiClientSettings(
                self_client_id=11,
                enable_service_ticket_checking=True,
                enable_user_ticket_checking=tp2.BlackboxEnv.Test,
                self_secret="AAAAAAAAAAAAAAAAAAAAAA",
                dsts={"bb": 224, "datasync": 2000060},
            )
        )

        self._allowed_consumers = kwargs['acl']  # array

    def stop(self):
        self._client.stop()

    # Processing of request is here
    def handle_request(self, **kwargs):
        try:
            st = self._client.check_service_ticket(kwargs['X-Ya-Service-Ticket'])
            ut = self._client.check_user_ticket(kwargs['X-Ya-User-Ticket'])

            if st.src not in self._allowed_consumers:
                raise Exception("Access denied (service)")

            if 'allow_to_get_secret_data' not in ut.scopes:
                raise Exception("Access denied (user)")

            return requests.get(
                'my_backend_request',
                headers={'X-Ya-Service-Ticket': self._client.get_service_ticket_for("datasync")},
            ).content
        except tp2.TvmException:
            raise Exception("Error")
