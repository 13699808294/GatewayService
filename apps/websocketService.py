import asyncio
import json
import os
import time
from json import JSONDecodeError

import tornado
import uvloop
from tornado import httpserver, gen
from tornado import web
from tornado.options import options
from tornado.platform.asyncio import BaseAsyncIOLoop
from tornado.web import StaticFileHandler

from apps.websocket.websocketClient import WebsocketClient
from setting.setting import DEBUG, ENVIRONMENT, BASE_DIR
from utils.logClient import logClient
from utils.my_json import json_dumps

tornado.options.define('websocket_service_port', type=int, default=8015, help='服务器端口号')


class WebsocketService():
    def __init__(self,ioloop = None,aioloop=None,mosquittoClient=None):
        self.ioloop = ioloop
        self.aioloop = aioloop
        self.mosquittoClient = mosquittoClient
        if self.ioloop == None:
            raise TypeError('ioloop error')
        if self.aioloop == None:
            raise TypeError('aioloop error')

        self.clientObjectSet = set()

        self.urlpatterns = [
            (r'/', WebsocketClient, {'server': self}),
            # 提供静态资源,调试时使用
            # (r'/(.*)', StaticFileHandler,{'path': os.path.join(os.path.dirname(__file__), 'static/html'), 'default_filename': 'index.html'})
        ]
        # self.ioloop.add_timeout(self.ioloop.time()+1,self.checkWebsocketClientHeart)
        ssl_options = {
            'certfile': '/ssl_file/websocket.pem',
            'keyfile': '/ssl_file/websocket.key'
        }
        app = web.Application(self.urlpatterns,
                              debug=DEBUG,
                              # autoreload=True,
                              # compiled_template_cache=False,
                              # static_hash_cache=False,
                              # serve_traceback=True,
                              static_path = os.path.join(os.path.dirname(__file__),'static'),
                              template_path = os.path.join(os.path.dirname(__file__),'template'),
                              autoescape=None,  # 全局关闭模板转义功能
                                      )
        # if DEBUG:
        wsServer = httpserver.HTTPServer(app)
        # else:
        #     wsServer = httpserver.HTTPServer(app, ssl_options=ssl_options)

        wsServer.listen(options.websocket_service_port)
        self.ioloop.add_timeout(self.ioloop.time() + 10, self.checkWebsocketClientHeart)





    @gen.coroutine
    def checkWebsocketClientHeart(self):
        now_time = time.time()
        for clientObject in self.clientObjectSet:
            if now_time - clientObject.lastHeartbeat >= 30:
                clientObject.close()
                self.clientObjectSet.discard(clientObject)
                del clientObject
                self.ioloop.add_timeout(self.ioloop.time() + 1, self.checkWebsocketClientHeart)
                break
        self.ioloop.add_timeout(self.ioloop.time() + 10, self.checkWebsocketClientHeart)

