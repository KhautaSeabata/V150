from flask import Flask, render_template_string

app = Flask(__name__)

@app.route('/')
def index():
    return render_template_string("""
        <!DOCTYPE html>
        <html>
        <head><title>Live Tick Stream</title></head>
        <body>
            <h1 style="text-align:center;">✅ Live tick data is being streamed to Firebase</h1>
        </body>
        </html>
    """)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)
