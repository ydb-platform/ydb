import os
import warnings
import tornado

from tornado import web, iostream
from openhtf.output.servers.station_server import StationServer as HTFStationServer
from .file_provider import TEMPFILES_PATH

class StationServer(HTFStationServer):
    def __init__(self, file_provider, *args, **kwargs):
        super(StationServer, self).__init__(*args, **kwargs)
        
        self.file_provider = file_provider
        
        self.application.add_handlers(
            r".*",  # match any host
            [
                (
                    TEMPFILES_PATH + r"([^/]*)",
                    TemporaryFileHandler,
                    { 'file_provider': self.file_provider }
                ),
            ]
        )

            
        
class TemporaryFileHandler(tornado.web.RequestHandler):
    def initialize(self, file_provider):
        self.files = file_provider.files
    
    async def get(self, *args, **kwargs):
        try:
            filename = self.files[self.request.path]
        except KeyError:
            raise tornado.web.HTTPError(404)
        
        await self._write_file_chunks(filename)
        
    async def _write_file_chunks(self, filename):
        # chunk size to read
        chunk_size = 1024 * 1024 * 1 # 1 MiB

        with open(filename, 'rb') as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                try:
                    self.write(chunk) # write the cunk to response
                    await self.flush() # flush the current chunk to socket
                except iostream.StreamClosedError:
                    # this means the client has closed the connection
                    # so break the loop
                    break
                finally:
                    # deleting the chunk is very important because 
                    # if many clients are downloading files at the 
                    # same time, the chunks in memory will keep 
                    # increasing and will eat up the RAM
                    del chunk