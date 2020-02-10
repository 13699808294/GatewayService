import asyncio

import uvloop
from tornado.platform.asyncio import BaseAsyncIOLoop

from apps.mosquittoClient.mosquittoClient import MosquittoClient
from apps.websocket.websocketClient import WebsocketClient
from apps.websocketService import WebsocketService

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())  # 修改循环策略为uvloop
    aioloop = asyncio.get_event_loop()  # 获取aioloop循环事件
    ioloop = BaseAsyncIOLoop(aioloop)  # 使用aioloop创建ioloop
    mosquittoClient = MosquittoClient(ioloop=ioloop, aioloop=aioloop)
    websocketService = WebsocketService(ioloop=ioloop, aioloop=aioloop,mosquittoClient=mosquittoClient)
    mosquittoClient.websocketObject = websocketService
    ioloop.current().start()