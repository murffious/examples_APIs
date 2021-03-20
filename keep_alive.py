from flask import Flask
from threading import Thread

app = Flask('')

@app.route('/')
def home():
    return "Hello. I am alive!"

def run():
  app.run(host='0.0.0.0',port=8080)

def keep_alive():
    t = Thread(target=run)
    t.start()



# Once deployed, the server will continue to run in the background, even after you close the browser tab. The server will stay awake and active until an hour after its last request, after which it will enter a sleeping stage. Sleeping repls will be woken up as soon as it receives another request; there is no need to re-run the repl. However, if you make changes to your server, you will need to restart the repl in order to see those changes reflected in the live version. Possible paid to keep not sleeping
