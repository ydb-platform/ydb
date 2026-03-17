import signal

from django.db.backends.base.client import BaseDatabaseClient


class DatabaseClient(BaseDatabaseClient):
    executable_name = "clickhouse-client"

    @classmethod
    def settings_to_cmd_args_env(cls, settings_dict, parameters):
        args = [cls.executable_name]
        options = settings_dict.get("OPTIONS", {})

        host = settings_dict.get("HOST")
        port = settings_dict.get("PORT")
        dbname = settings_dict.get("NAME")
        user = settings_dict.get("USER")
        passwd = settings_dict.get("PASSWORD")
        secure = options.get("secure")
        dsn = options.get("dsn")

        if dsn:
            args += [dsn]
        else:
            if host:
                args += ["-h", host]
            if port:
                args += ["--port", str(port)]
            if user:
                args += ["-u", user]
            if passwd:
                args += ["--password", passwd]
            if dbname:
                args += ["-d", dbname]
            if secure:
                args += ["--secure"]
        args.extend(parameters)
        return args, None

    def runshell(self, parameters):
        sigint_handler = signal.getsignal(signal.SIGINT)
        try:
            # Allow SIGINT to pass to psql to abort queries.
            signal.signal(signal.SIGINT, signal.SIG_IGN)
            super().runshell(parameters)
        finally:
            # Restore the original SIGINT handler.
            signal.signal(signal.SIGINT, sigint_handler)
