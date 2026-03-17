import typing

from websockets.exceptions import ConnectionClosedOK
from websockets.legacy.client import connect


class WebsocketProxy:
    def __init__(self, ws):
        self.ws = ws
        self.opened = True
        self.client_received: typing.List[str] = []
        self.client_sent: typing.List[str] = []

    @property
    def server_received(self):
        return self.client_sent

    @property
    def server_sent(self):
        return self.client_received


async def websocket_proxy(url, *args, **kwargs) -> WebsocketProxy:
    mimic = kwargs.pop("mimic", None)
    async with connect(url, *args, **kwargs) as websocket:
        ws_proxy = WebsocketProxy(websocket)

        if mimic:
            do_send = websocket.send
            do_recv = websocket.recv

            async def send(data):
                ws_proxy.client_sent.append(data)
                await do_send(data)

            async def recv():
                message = await do_recv()
                ws_proxy.client_received.append(message)
                return message

            websocket.send = send  # type: ignore
            websocket.recv = recv  # type: ignore

            try:
                await mimic(websocket)
            except ConnectionClosedOK:
                pass
            else:
                await websocket.send("")
    return ws_proxy
