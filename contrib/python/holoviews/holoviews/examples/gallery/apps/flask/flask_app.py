from bokeh.client import pull_session
from bokeh.embed import server_session
from flask import Flask, render_template

app = Flask(__name__)

# locally creates a page
@app.route('/')
def index():
    with pull_session(url="http://localhost:5006/") as session:
            # generate a script to load the customized session
            script = server_session(session_id=session.id, url='http://localhost:5006')
            # use the script in the rendered page
    return render_template("embed.html", script=script, template="Flask")


if __name__ == '__main__':
    # runs app in debug mode
    app.run(port=5000, debug=True)
