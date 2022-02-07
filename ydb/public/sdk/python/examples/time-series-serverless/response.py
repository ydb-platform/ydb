import json


class Response:
    status = 200
    headers = {
        "Content-type": "application/json",
    }
    body = {}

    def as_dict(self):
        return dict(
            status=self.status, headers=self.headers, body=json.dumps(self.body)
        )


class Ok(Response):
    pass


class ErrorResponse(Response):
    def __init__(self, message: str):
        self.body["message"] = message


class Conflict(ErrorResponse):
    status = 409


class BadRequest(ErrorResponse):
    status = 400
