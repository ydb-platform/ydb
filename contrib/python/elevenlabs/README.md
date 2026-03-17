# ElevenLabs Python Library

![LOGO](https://github.com/elevenlabs/elevenlabs-python/assets/12028621/21267d89-5e82-4e7e-9c81-caf30b237683)

[![fern shield](https://img.shields.io/badge/%F0%9F%8C%BF-SDK%20generated%20by%20Fern-brightgreen)](https://buildwithfern.com/?utm_source=fern-elevenlabs/elevenlabs-python/readme)
[![Discord](https://badgen.net/badge/black/ElevenLabs/icon?icon=discord&label)](https://discord.gg/elevenlabs)
[![Twitter](https://badgen.net/badge/black/elevenlabsio/icon?icon=twitter&label)](https://twitter.com/elevenlabsio)
[![PyPI - Python Version](https://img.shields.io/pypi/v/elevenlabs?style=flat&colorA=black&colorB=black)](https://pypi.org/project/elevenlabs/)
[![Downloads](https://static.pepy.tech/personalized-badge/elevenlabs?period=total&units=international_system&left_color=black&right_color=black&left_text=Downloads)](https://pepy.tech/project/elevenlabs)

The official Python SDK for [ElevenLabs](https://elevenlabs.io/). ElevenLabs brings the most compelling, rich and lifelike voices to creators and developers in just a few lines of code.

## 📖 API & Docs

Check out the [HTTP API documentation](https://elevenlabs.io/docs/api-reference).

## Install

```bash
pip install elevenlabs
```

## Usage

### Main Models

1. **Eleven v3** (`eleven_v3`)
    - Dramatic delivery and performances
    - 70+ languages supported
    - Supported for natural multi-speaker dialogue

2. **Eleven Multilingual v2** (`eleven_multilingual_v2`)

    - Excels in stability, language diversity, and accent accuracy
    - Supports 29 languages
    - Recommended for most use cases

3. **Eleven Flash v2.5** (`eleven_flash_v2_5`)

    - Ultra-low latency
    - Supports 32 languages
    - Faster model, 50% lower price per character

4. **Eleven Turbo v2.5** (`eleven_turbo_v2_5`)

    - Good balance of quality and latency
    - Ideal for developer use cases where speed is crucial
    - Supports 32 languages

For more detailed information about these models and others, visit the [ElevenLabs Models documentation](https://elevenlabs.io/docs/models).

```py
from dotenv import load_dotenv
from elevenlabs.client import ElevenLabs
from elevenlabs.play import play

load_dotenv()

elevenlabs = ElevenLabs()

audio = elevenlabs.text_to_speech.convert(
    text="The first move is what sets everything in motion.",
    voice_id="JBFqnCBsd6RMkjVDRZzb",
    model_id="eleven_v3",
    output_format="mp3_44100_128",
)

play(audio)
```

<details> <summary> Play </summary>

🎧 **Try it out!** Want to hear our voices in action? Visit the [ElevenLabs Voice Lab](https://elevenlabs.io/voice-lab) to experiment with different voices, languages, and settings.

</details>

## Voices

List all your available voices with `search()`.

```py
from elevenlabs.client import ElevenLabs

elevenlabs = ElevenLabs(
  api_key="YOUR_API_KEY",
)

response = elevenlabs.voices.search()
print(response.voices)
```

For information about the structure of the voices output, please refer to the [official ElevenLabs API documentation for Get Voices](https://elevenlabs.io/docs/api-reference/get-voices).

Build a voice object with custom settings to personalize the voice style, or call
`elevenlabs.voices.settings.get("your-voice-id")` to get the default settings for the voice.

</details>

## Clone Voice

Clone your voice in an instant. Note that voice cloning requires an API key, see below.

```py
from elevenlabs.client import ElevenLabs
from elevenlabs.play import play

elevenlabs = ElevenLabs(
  api_key="YOUR_API_KEY",
)

voice = elevenlabs.voices.ivc.create(
    name="Alex",
    description="An old American male voice with a slight hoarseness in his throat. Perfect for news", # Optional
    files=["./sample_0.mp3", "./sample_1.mp3", "./sample_2.mp3"],
)
```

## Streaming

Stream audio in real-time, as it's being generated.

```py
from elevenlabs import stream
from elevenlabs.client import ElevenLabs

elevenlabs = ElevenLabs(
  api_key="YOUR_API_KEY",
)

audio_stream = elevenlabs.text_to_speech.stream(
    text="This is a test",
    voice_id="JBFqnCBsd6RMkjVDRZzb",
    model_id="eleven_multilingual_v2"
)

# option 1: play the streamed audio locally
stream(audio_stream)

# option 2: process the audio bytes manually
for chunk in audio_stream:
    if isinstance(chunk, bytes):
        print(chunk)

```

## Async Client

Use `AsyncElevenLabs` if you want to make API calls asynchronously.

```python
import asyncio

from elevenlabs.client import AsyncElevenLabs

elevenlabs = AsyncElevenLabs(
  api_key="MY_API_KEY"
)

async def print_models() -> None:
    models = await elevenlabs.models.list()
    print(models)

asyncio.run(print_models())
```

## ElevenAgents

Build interactive AI agents with real-time audio capabilities using ElevenAgents.

### Basic Usage

```python
from elevenlabs.client import ElevenLabs
from elevenlabs.conversational_ai.conversation import Conversation, ClientTools
from elevenlabs.conversational_ai.default_audio_interface import DefaultAudioInterface

elevenlabs = ElevenLabs(
  api_key="YOUR_API_KEY",
)

# Create audio interface for real-time audio input/output
audio_interface = DefaultAudioInterface()

# Create conversation
conversation = Conversation(
    client=elevenlabs,
    agent_id="your-agent-id",
    requires_auth=True,
    audio_interface=audio_interface,
)

# Start the conversation
conversation.start_session()

# The conversation runs in background until you call:
conversation.end_session()
```

### Custom Event Loop Support

For advanced use cases involving context propagation, resource reuse, or specific event loop management, `ClientTools` supports custom asyncio event loops:

```python
import asyncio
from elevenlabs.conversational_ai.conversation import ClientTools

elevenlabs = ElevenLabs(
  api_key="YOUR_API_KEY",
)

async def main():
    # Get the current event loop
    custom_loop = asyncio.get_running_loop()

    # Create ClientTools with custom loop to prevent "different event loop" errors
    client_tools = ClientTools(loop=custom_loop)

    # Register your tools
    async def get_weather(params):
        location = params.get("location", "Unknown")
        # Your async logic here
        return f"Weather in {location}: Sunny, 72°F"

    client_tools.register("get_weather", get_weather, is_async=True)

    # Use with conversation
    conversation = Conversation(
        client=elevenlabs,
        agent_id="your-agent-id",
        requires_auth=True,
        audio_interface=audio_interface,
        client_tools=client_tools
    )

asyncio.run(main())
```

**Benefits of Custom Event Loop:**
- **Context Propagation**: Maintain request-scoped state across async operations
- **Resource Reuse**: Share existing async resources like HTTP sessions or database pools
- **Loop Management**: Prevent "Task got Future attached to a different event loop" errors
- **Performance**: Better control over async task scheduling and execution

**Important:** When using a custom loop, you're responsible for its lifecycle
Don't close the loop while ClientTools are still using it.

### Tool Registration

Register custom tools that the AI agent can call during conversations:

```python
client_tools = ClientTools()

# Sync tool
def calculate_sum(params):
    numbers = params.get("numbers", [])
    return sum(numbers)

# Async tool
async def fetch_data(params):
    url = params.get("url")
    # Your async HTTP request logic
    return {"data": "fetched"}

client_tools.register("calculate_sum", calculate_sum, is_async=False)
client_tools.register("fetch_data", fetch_data, is_async=True)
```

## Languages Supported

Explore [all models & languages](https://elevenlabs.io/docs/models).

## Contributing

While we value open-source contributions to this SDK, this library is generated programmatically. Additions made directly to this library would have to be moved over to our generation code, otherwise they would be overwritten upon the next generated release. Feel free to open a PR as a proof of concept, but know that we will not be able to merge it as-is. We suggest opening an issue first to discuss with us!

On the other hand, contributions to the README are always very welcome!
