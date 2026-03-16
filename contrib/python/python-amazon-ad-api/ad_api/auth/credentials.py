class Credentials:
    def __init__(self, credentials):
        self.client_id = credentials['client_id']
        self.client_secret = credentials['client_secret']
        self.refresh_token = credentials['refresh_token']
        self.profile_id = credentials.get('profile_id')
