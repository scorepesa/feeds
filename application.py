#!/usr/bin/env python
from flask import Flask
import logging
import logging.handlers
from flask_restful import Api
import os
from api import ProcessQueue, Wekawekapesa, WekawekapesaDemo

cur_dir = os.path.dirname(__file__)

filename = '/var/log/shikabet/shikabet_queue_api.log'

#attributes
app = Flask(__name__)
api = Api(app)

app.add_url_rule('/queue/process', view_func=ProcessQueue.as_view('process_queue'))
api.add_resource(Wekawekapesa, '/kibeti')
api.add_resource(WekawekapesaDemo,'/demo_depo')

log_formatter = logging.Formatter("%(asctime)s %(levelname)-8s %(name)-5s %(filename)s:%(lineno)d:%(funcName)-10s %(message)s", datefmt="%m-%d-%y %H:%M:%S")

app.logger.setLevel(logging.DEBUG)
handler = logging.handlers.SysLogHandler(address = '/dev/log')
handler.setFormatter(log_formatter)
app.logger.addHandler(handler)


handler2 = logging.handlers.RotatingFileHandler(filename,
    maxBytes=50*1024*1024, backupCount=5)
handler2.setFormatter(log_formatter)
app.logger.addHandler(handler2)

if __name__ == '__main__':
    app.run(port=5000)

