# FastAPI CLI

<a href="https://github.com/fastapi/fastapi-cli/actions/workflows/test.yml" target="_blank">
    <img src="https://github.com/fastapi/fastapi-cli/actions/workflows/test.yml/badge.svg" alt="Test">
</a>
<a href="https://github.com/fastapi/fastapi-cli/actions/workflows/publish.yml" target="_blank">
    <img src="https://github.com/fastapi/fastapi-cli/actions/workflows/publish.yml/badge.svg" alt="Publish">
</a>
<a href="https://coverage-badge.samuelcolvin.workers.dev/redirect/fastapi/fastapi-cli" target="_blank">
    <img src="https://coverage-badge.samuelcolvin.workers.dev/fastapi/fastapi-cli.svg" alt="Coverage">
<a href="https://pypi.org/project/fastapi-cli" target="_blank">
    <img src="https://img.shields.io/pypi/v/fastapi-cli?color=%2334D058&label=pypi%20package" alt="Package version">
</a>

---

**Source Code**: <a href="https://github.com/fastapi/fastapi-cli" target="_blank">https://github.com/fastapi/fastapi-cli</a>

---

Run and manage FastAPI apps from the command line with FastAPI CLI. 🚀

## Description

**FastAPI CLI** is a command line program `fastapi` that you can use to serve your FastAPI app, manage your FastAPI project, and more.

When you install FastAPI (e.g. with `pip install "fastapi[standard]"`), it includes a package called `fastapi-cli`, this package provides the `fastapi` command in the terminal.

To run your FastAPI app for development, you can use the `fastapi dev` command:

<div class="termy">

```console
$ fastapi dev main.py

   FastAPI   Starting development server 🚀

             Searching for package file structure from directories with __init__.py files
             Importing from /home/user/code/awesomeapp

    module   🐍 main.py

      code   Importing the FastAPI app object from the module with the following code:

             from main import app

       app   Using import string: main:app

    server   Server started at http://127.0.0.1:8000
    server   Documentation at http://127.0.0.1:8000/docs

       tip   Running in development mode, for production use: fastapi run

             Logs:

      INFO   Will watch for changes in these directories: ['/home/user/code/awesomeapp']
      INFO   Uvicorn running on http://127.0.0.1:8000 (Press CTRL+C to quit)
      INFO   Started reloader process [4106097] using WatchFiles
      INFO   Started server process [4106120]
      INFO   Waiting for application startup.
      INFO   Application startup complete.
```

</div>

That command line program called `fastapi` is **FastAPI CLI**.

FastAPI CLI takes the path to your Python program and automatically detects the variable with the FastAPI (commonly named `app`) and how to import it, and then serves it.

For production you would use `fastapi run` instead. 🚀

Internally, **FastAPI CLI** uses <a href="https://www.uvicorn.org" class="external-link" target="_blank">Uvicorn</a>, a high-performance, production-ready, ASGI server. 😎

## `fastapi dev`

When you run `fastapi dev`, it will run on development mode.

By default, it will have **auto-reload** enabled, so it will automatically reload the server when you make changes to your code. This is resource intensive and could be less stable than without it, you should only use it for development.

By default it will listen on the IP address `127.0.0.1`, which is the IP for your machine to communicate with itself alone (`localhost`).

## `fastapi run`

When you run `fastapi run`, it will run on production mode by default.

It will have **auto-reload disabled** by default.

It will listen on the IP address `0.0.0.0`, which means all the available IP addresses, this way it will be publicly accessible to anyone that can communicate with the machine. This is how you would normally run it in production, for example, in a container.

In most cases you would (and should) have a "termination proxy" handling HTTPS for you on top, this will depend on how you deploy your application, your provider might do this for you, or you might need to set it up yourself. You can learn more about it in the <a href="https://fastapi.tiangolo.com/deployment/" class="external-link" target="_blank">FastAPI Deployment documentation</a>.

## License

This project is licensed under the terms of the MIT license.
