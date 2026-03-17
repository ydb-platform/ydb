import argparse


def parse_args():
    argument_parser = argparse.ArgumentParser()
    argument_parser.add_argument(
        '-c', '--cls', help='message class to use for decoding the data'
    )
    argument_parser.add_argument('-d', '--data', help='data dump file')
    argument_parser.add_argument(
        '-f', '--format', default='hex', help='data file format: hex, pcap'
    )
    argument_parser.add_argument(
        '-m', '--match', help='match protocol family (only for pcap data)'
    )
    argument_parser.add_argument(
        '-o',
        '--offset',
        help='message offset in the data',
        default=0,
        type=int,
    )
    argument_parser.add_argument(
        '-k', '--key', help='key format (see struct)', default='H'
    )
    return argument_parser.parse_args()
