import datetime
import time
import uuid
from json import JSONDecodeError
from urllib.parse import urlparse

import tornado.websocket
from tornado import gen
import json

from setting.setting import MY_SQL_SERVER_HOST, DATABASES, ENVIRONMENT, MEETING_ROOM_GUID
from utils.MysqlClient import MysqlClient
from utils.logClient import logClient
from utils.my_json import json_dumps
from tornado.websocket import WebSocketClosedError

class WebsocketClient(tornado.websocket.WebSocketHandler):
    def check_origin(self, origin):
        parsed_origin = urlparse(origin)
        origin = parsed_origin.netloc
        origin = origin.lower()

        host = self.request.headers.get("Host")
        # Check to see that origin matches host directly, including ports
        self.host = host
        origin = origin.split(':')[0]
        host = host.split(':')[0]
        # if ENVIRONMENT == 'build':
        #     return origin == host
        # else:
        return True


    def initialize(self, server):
        self.server = server        #WebsocketServer对象
        self.ioloop = server.ioloop #
        self.aioloop = server.aioloop
        self.mosquittoClient = server.mosquittoClient
        self.lastHeartbeat = time.time()
        self.host = None
        self.login = None
        self.meeting_room_list = []

    @gen.coroutine
    def open(self):
        self.server.clientObjectSet.add(self)
        yield logClient.tornadoDebugLog('websocket连接接入,当前连接数量:{}'.format(len(self.server.clientObjectSet)))


    @gen.coroutine
    def sendToClientService(self,msg):
        try:
            self.write_message(msg)
        except Exception as e:
            yield logClient.tornadoErrorLog(e)
            return

    @gen.coroutine
    def on_message(self, data):
        '''
        1>接受前端的信息,2>发送给websocketService,3>接受websocketService的消息,4>分发给前端
        '''
        try:
            msg = json.loads(data)
        except JSONDecodeError as e:
            try:
                msg = eval(data)
            except:
                return

        if not isinstance(msg,dict):
            return
        yield logClient.tornadoDebugLog('1---中控客户端信息:{}'.format(msg))
        msg_type = msg.get('type')
        if msg_type == 'login':
            self.meeting_room_list = msg.get('meeting_room_list')
            if self.login == None:
                self.login = True
                data = {
                    'type': 'update_device',
                    'meeting_room_guid': None
                }
                self.sendToClientService(json_dumps(data))
        if not self.login:
            return

        if msg_type == 'heartbeat':
            # self.lastHeartbeat = time.time()
            self.lastHeartbeat = msg.get('time')

        if msg_type == 'channel_status':
            #收到中控的通道反馈信息,通过网关同步回去
            topic = msg.get('topic')
            data = msg.get('data')
            topic_list = self.TopicToList(topic)
            self.mosquittoClient.handleChannelSync(topic,topic_list, data)
        elif msg_type == 'update_device':
            yield logClient.tornadoInfoLog('从中控中获取设备更新信息')
            self.mosquittoClient.updateMeetingRoomDevice(msg.get('meeting_room_device_list'))
        elif msg_type == 'sync_device':
            yield logClient.tornadoInfoLog('从中控中获取设备同步信息')
            self.mosquittoClient.syncMeetingRoomDevice(msg.get('meeting_room_device_list'))


    @gen.coroutine
    def on_close(self):
        #从server的websocket对象集合中移除自己
        self.server.clientObjectSet.discard(self)
        yield logClient.tornadoInfoLog('用户退出登录,当前连接数量:{}'.format(len(self.server.clientObjectSet)))
        self = None

    def TopicToList(self,topic):
        topic_list = topic.split('/')
        while '' in topic_list:
            topic_list.remove('')
        return topic_list
