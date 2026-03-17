import os


class Credentials:
    def __init__(self, refresh_token, credentials):
        self.client_id = credentials.lwa_app_id
        self.client_secret = credentials.lwa_client_secret
        self.refresh_token = refresh_token or credentials.refresh_token
