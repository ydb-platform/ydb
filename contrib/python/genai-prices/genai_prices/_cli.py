from __future__ import annotations as _annotations

import argparse
import sys
from collections.abc import Sequence
from datetime import datetime

import pydantic

from . import Usage, __version__, calc_price, update_prices


def cli() -> int:  # pragma: no cover
    """Run the CLI."""
    sys.exit(cli_logic())


def cli_logic(args_list: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog='genai-prices',
        description=f'genai-prices CLI v{__version__}\n\nCalculate prices for calling LLM inference APIs.\n',
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument('--version', action='store_true', help='Show version and exit')
    subparsers = parser.add_subparsers(dest='command')

    calc_parser = subparsers.add_parser('calc', help='Calculate prices.', description='Calculate prices.')
    calc_parser.add_argument(
        'model',
        nargs='+',
        help='Model and optionally provider used: either just the model ID, e.g. "gpt-4o" or in format "<provider>:<model>" e.g. "openai:gpt-4o".',
    )
    calc_parser.add_argument(
        '--update-prices', action='store_true', help='Whether to update the model prices from GitHub.'
    )
    calc_parser.add_argument(
        '--timestamp',
        help='Timestamp of the request, in RFC 3339 format, if not provided, the current time will be used.',
    )

    calc_parser.add_argument('--input-tokens', type=int, help='Usage: Number of text input/prompt tokens.')
    calc_parser.add_argument('--cache-write-tokens', type=int, help='Usage: Number of tokens written to the cache.')
    calc_parser.add_argument('--cache-read-tokens', type=int, help='Usage: Number of tokens read from the cache.')
    calc_parser.add_argument('--output-tokens', type=int, help='Usage: Number of text output/completion tokens.')
    calc_parser.add_argument('--input-audio-tokens', type=int, help='Usage: Number of audio input tokens.')
    calc_parser.add_argument(
        '--cache-audio-read-tokens', type=int, help='Usage: Number of audio tokens read from the cache.'
    )
    calc_parser.add_argument('--output-audio-tokens', type=int, help='Usage: Number of output audio tokens.')

    list_parser = subparsers.add_parser(
        'list', help='List providers and models.', description='List providers and models.'
    )
    list_parser.add_argument('provider', nargs='?', help='Only list models for the provider.')

    args = parser.parse_args(args_list)
    if args.version:
        print(f'genai-prices {__version__}')
        return 0

    if args.command == 'calc':
        return calc_prices(args)
    elif args.command == 'list':
        return list_models(args)
    else:
        parser.print_help()
        return 1


def calc_prices(args: argparse.Namespace) -> int:
    usage = Usage(
        input_tokens=args.input_tokens,
        cache_write_tokens=args.cache_write_tokens,
        cache_read_tokens=args.cache_read_tokens,
        output_tokens=args.output_tokens,
        input_audio_tokens=args.input_audio_tokens,
        cache_audio_read_tokens=args.cache_audio_read_tokens,
        output_audio_tokens=args.output_audio_tokens,
    )
    for model in args.model:
        provider_id = None
        if ':' in model:
            provider_id, model = model.split(':', 1)

        genai_request_timestamp = None
        if args.timestamp:
            genai_request_timestamp = pydantic.TypeAdapter(datetime).validate_python(args.timestamp)

        if args.update_prices:
            price_update = update_prices.UpdatePrices()
            price_update.start(wait=True)
        price_calc = calc_price(
            usage,
            model_ref=model,
            provider_id=provider_id,
            genai_request_timestamp=genai_request_timestamp,
        )
        w = price_calc.model.context_window
        output: list[tuple[str, str | None]] = [
            ('Provider', price_calc.provider.name),
            ('Model', price_calc.model.name or price_calc.model.id),
            ('Model Prices', str(price_calc.model_price)),
            ('Context Window', f'{w:,d}' if w is not None else None),
            ('Input Price', f'${price_calc.input_price}'),
            ('Output Price', f'${price_calc.output_price}'),
            ('Total Price', f'${price_calc.total_price}'),
        ]
        for key, value in output:
            if value is not None:
                print(f'{key:>14}: {value}')
        print('')
    return 0


def list_models(args: argparse.Namespace) -> int:
    from .data import providers

    if args.provider:
        provider_ids = {p.id for p in providers}
        if args.provider not in provider_ids:
            print(f'Error: provider {args.provider!r} not found in {sorted(provider_ids)}', file=sys.stderr)
            return 1

    for provider in providers:
        if args.provider and provider.id != args.provider:
            continue
        print(f'{provider.name}: ({len(provider.models)} models)')
        for model in provider.models:
            print(f'  {provider.id}:{model.id}', end='')
            if model.name:
                print(f': {model.name}')
            else:
                print('')
    return 0
