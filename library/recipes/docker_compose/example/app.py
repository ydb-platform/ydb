import time

import redis
from flask import Flask


app = Flask(__name__)
cache = redis.Redis(host='redis', port=6379)


@app.route('/')
def hello():
    return 'Hello World!'


if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True)
