# -*- coding: utf-8 -*-
from croniter.croniter import croniter
from aiocron import asyncio
from aiocron import crontab
import subprocess
import argparse


def main():
    parser = argparse.ArgumentParser()
    parser.prog = "python -m aiocron"
    parser.add_argument(
        "-n",
        type=int,
        default=1,
        help="loop N times. 0 for infinite loop",
    )
    parser.add_argument("crontab", help='quoted crontab. like "* * * * *"')
    parser.add_argument("command", nargs="+", help="shell command to run")
    args = parser.parse_args()

    cron = args.crontab
    try:
        croniter(cron)
    except ValueError:
        parser.error("Invalid cron format")

    cmd = args.command

    loop = asyncio.get_event_loop()

    def calback():
        subprocess.call(cmd)
        if args.n != 0:
            cron.n -= 1
            if not cron.n:
                loop.stop()

    cron = crontab(cron, func=calback)
    cron.n = args.n

    cron.start()
    try:
        loop.run_forever()
    except:
        pass


if __name__ == "__main__":  # pragma: no cover
    main()
