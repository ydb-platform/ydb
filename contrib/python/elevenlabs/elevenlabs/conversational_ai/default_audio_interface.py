from typing import Callable, Awaitable
import queue
import threading
import asyncio

from .conversation import AudioInterface, AsyncAudioInterface


class DefaultAudioInterface(AudioInterface):
    INPUT_FRAMES_PER_BUFFER = 4000  # 250ms @ 16kHz
    OUTPUT_FRAMES_PER_BUFFER = 1000  # 62.5ms @ 16kHz

    def __init__(self):
        try:
            import pyaudio
        except ImportError:
            raise ImportError("To use DefaultAudioInterface you must install pyaudio.")
        self.pyaudio = pyaudio

    def start(self, input_callback: Callable[[bytes], None]):
        # Audio input is using callbacks from pyaudio which we simply pass through.
        self.input_callback = input_callback

        # Audio output is buffered so we can handle interruptions.
        # Start a separate thread to handle writing to the output stream.
        self.output_queue: queue.Queue[bytes] = queue.Queue()
        self.should_stop = threading.Event()
        self.output_thread = threading.Thread(target=self._output_thread)

        self.p = self.pyaudio.PyAudio()
        self.in_stream = self.p.open(
            format=self.pyaudio.paInt16,
            channels=1,
            rate=16000,
            input=True,
            stream_callback=self._in_callback,
            frames_per_buffer=self.INPUT_FRAMES_PER_BUFFER,
            start=True,
        )
        self.out_stream = self.p.open(
            format=self.pyaudio.paInt16,
            channels=1,
            rate=16000,
            output=True,
            frames_per_buffer=self.OUTPUT_FRAMES_PER_BUFFER,
            start=True,
        )

        self.output_thread.start()

    def stop(self):
        self.should_stop.set()
        self.output_thread.join()
        self.in_stream.stop_stream()
        self.in_stream.close()
        self.out_stream.close()
        self.p.terminate()

    def output(self, audio: bytes):
        self.output_queue.put(audio)

    def interrupt(self):
        # Clear the output queue to stop any audio that is currently playing.
        # Note: We can't atomically clear the whole queue, but we are doing
        # it from the message handling thread so no new audio will be added
        # while we are clearing.
        try:
            while True:
                _ = self.output_queue.get(block=False)
        except queue.Empty:
            pass

    def _output_thread(self):
        while not self.should_stop.is_set():
            try:
                audio = self.output_queue.get(timeout=0.25)
                self.out_stream.write(audio)
            except queue.Empty:
                pass

    def _in_callback(self, in_data, frame_count, time_info, status):
        if self.input_callback:
            self.input_callback(in_data)
        return (None, self.pyaudio.paContinue)


class AsyncDefaultAudioInterface(AsyncAudioInterface):
    INPUT_FRAMES_PER_BUFFER = 4000  # 250ms @ 16kHz
    OUTPUT_FRAMES_PER_BUFFER = 1000  # 62.5ms @ 16kHz

    def __init__(self):
        try:
            import pyaudio
        except ImportError:
            raise ImportError("To use AsyncDefaultAudioInterface you must install pyaudio.")
        self.pyaudio = pyaudio

    async def start(self, input_callback: Callable[[bytes], Awaitable[None]]):
        # Audio input is using callbacks from pyaudio which we adapt to async
        self.input_callback = input_callback

        # Audio output is buffered so we can handle interruptions.
        # Start a separate task to handle writing to the output stream.
        self.output_queue: asyncio.Queue[bytes] = asyncio.Queue()
        self.should_stop = asyncio.Event()
        
        self.p = self.pyaudio.PyAudio()
        self.in_stream = self.p.open(
            format=self.pyaudio.paInt16,
            channels=1,
            rate=16000,
            input=True,
            stream_callback=self._in_callback,
            frames_per_buffer=self.INPUT_FRAMES_PER_BUFFER,
            start=True,
        )
        self.out_stream = self.p.open(
            format=self.pyaudio.paInt16,
            channels=1,
            rate=16000,
            output=True,
            frames_per_buffer=self.OUTPUT_FRAMES_PER_BUFFER,
            start=True,
        )

        # Start the output task
        self.output_task = asyncio.create_task(self._output_task())

    async def stop(self):
        self.should_stop.set()
        await self.output_task
        self.in_stream.stop_stream()
        self.in_stream.close()
        self.out_stream.close()
        self.p.terminate()

    async def output(self, audio: bytes):
        await self.output_queue.put(audio)

    async def interrupt(self):
        # Clear the output queue to stop any audio that is currently playing.
        try:
            while True:
                try:
                    _ = self.output_queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
        except AttributeError:
            # In Python 3.8, it's asyncio.QueueEmpty, in 3.10+ it's asyncio.QueueEmpty
            while not self.output_queue.empty():
                try:
                    _ = self.output_queue.get_nowait()
                except:
                    break

    async def _output_task(self):
        while not self.should_stop.is_set():
            try:
                audio = await asyncio.wait_for(self.output_queue.get(), timeout=0.25)
                self.out_stream.write(audio)
            except asyncio.TimeoutError:
                pass

    def _in_callback(self, in_data, frame_count, time_info, status):
        if self.input_callback:
            # Schedule the async callback to run in the event loop
            try:
                loop = asyncio.get_event_loop()
                asyncio.run_coroutine_threadsafe(self.input_callback(in_data), loop)
            except RuntimeError:
                # No event loop running, ignore
                pass
        return (None, self.pyaudio.paContinue)
