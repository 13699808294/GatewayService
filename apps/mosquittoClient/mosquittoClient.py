import asyncio
import json
import os
import time
from json import JSONDecodeError

import paho.mqtt.client as mqtt
from tornado import gen

from setting.setting import QOS, MQTT_SERVICE_HOST, BASE_DIR, MEETING_ROOM_GUID
from utils.logClient import logClient
from utils.my_json import json_dumps

class MosquittoClient():
    def __init__(self,host=MQTT_SERVICE_HOST,port=1883,username='test001',password='test001',keepalive=60,bind_address='',ioloop=None,aioloop=None):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.keepalive = keepalive
        self.bind_address = bind_address
        self.ioloop = ioloop
        self.aioloop = aioloop
        self.connect_status = None
        if self.ioloop == None:
            raise TypeError('ioloop error')
        if self.aioloop == None:
            raise TypeError('aioloop error')

        self.mosquittoReceiveBuffer = []
        self.handle_message_task = None
        self.publish_buffer = []
        self.publish_buffer_task = None

        self.meeting_room_dict = {}
        self.macro_dict = {}

        self.test_count = 0

        self.mosquittoClient = mqtt.Client(client_id="", clean_session=True, userdata=None, protocol=mqtt.MQTTv311, transport="tcp")
        self.mosquittoClient.username_pw_set(username=self.username, password=self.password)
        # ssl
        if port == 8883:
            self.mosquittoClient.tls_set(ca_certs=os.path.join(BASE_DIR,'mosquitto_ssl_file/ca/ca.crt'),
                                         certfile=os.path.join(BASE_DIR,'mosquitto_ssl_file/server/server.crt'),
                                         keyfile=os.path.join(BASE_DIR,'mosquitto_ssl_file/server/server.key')
                                         )
            self.mosquittoClient.max_inflight_messages_set(200)
            self.mosquittoClient.max_queued_messages_set(1000)

        self.mosquittoClient.on_connect = self.on_connect
        self.mosquittoClient.on_disconnect = self.on_disconnect
        self.mosquittoClient.on_message = self.on_message

        self.websocketObject = None
        try:
            self.mosquittoClient.connect(host=self.host,port=self.port,keepalive=self.keepalive,bind_address=self.bind_address)
        except:
            logClient.debugLog('mqtt连接失败')
            self.ioloop.add_timeout(self.ioloop.time() + 5, self.keepConnect)

        self.ioloop.add_timeout(self.ioloop.time(), self.mosquittoClientLoop)
        self.ioloop.add_timeout(self.ioloop.time(), self.sendHeart)


    @gen.coroutine
    def my_publish(self,topic, payload=None, qos=0, retain=False):
        if self.connect_status:
            self.mosquittoClient.publish(topic,payload,qos,retain)
        else:
            msg = {
                'topic':topic,
                'payload':payload,
                'qos':qos,
                'retain':retain,
            }
            self.publish_buffer.append(msg)
        if self.publish_buffer_task == None:
            self.publish_buffer_task = self.ioloop.add_timeout(self.ioloop.time(), self.publish_buffer_info)

    @gen.coroutine
    def publish_buffer_info(self):
        if self.connect_status:
            while True:
                try:
                    msg = self.publish_buffer.pop(0)
                except:
                    self.publish_buffer_task = None
                    return
                self.mosquittoClient.publish(**msg)
        else:
            self.publish_buffer_task = self.ioloop.add_timeout(self.ioloop.time()+1, self.publish_buffer_info)

    #todo:收到信息回调
    def on_message(self,client, userdata, message):
        # 1 获取主题,数据
        topic = message.topic
        topic_list = self.TopicToList(topic)
        logClient.tornadoDebugLog(topic+' '+message.payload.decode())
        if len(topic_list) < 6:
            return
        data = {
            'topic': topic,
            'data': message.payload.decode(),
            'topic_list': topic_list
        }
        self.mosquittoReceiveBuffer.append(data)
        if self.handle_message_task == None:
            self.handle_message_task = 1
            asyncio.ensure_future(self.handle_message(), loop=self.aioloop).add_done_callback(self.handleMessageCallback)

    @gen.coroutine
    def handleMessageCallback(self, futu):
        self.handle_message_task = None

    @gen.coroutine
    def handle_message(self):
        try:
            while self.mosquittoReceiveBuffer:
                try:
                    msg = self.mosquittoReceiveBuffer.pop(0)
                except:
                    return

                topic = msg.get('topic')
                topic_list = msg.get('topic_list')
                control_event = msg.get('data')
                data = {
                    'type':'control',
                    'topic':topic,
                    'topic_list':topic_list,
                    'control_event':control_event
                }
                meeting_room_guid = topic_list[1]
                type_ = topic_list[5]
                if type_ == 'command':
                    channel = topic_list[7]
                    if channel == 'macro':
                        #计算macro信息
                        try:
                            macro_info = json.loads(control_event)
                        except:
                            macro_info = []
                        yield self.updateMeetingRoomMacro(meeting_room_guid,macro_info)

                        data = {
                            'type':'sync_device',
                            'meeting_room_guid':meeting_room_guid
                        }
                        yield logClient.tornadoDebugLog(json_dumps(data))
                        for websocketObject in self.websocketObject.clientObjectSet:
                            if meeting_room_guid in websocketObject.meeting_room_list:
                                websocketObject.sendToClientService(json_dumps(data))
                else:
                    yield logClient.tornadoDebugLog(json_dumps(data))
                    if topic_list[5] == 'macro':
                        yield self.macroControlHandle(meeting_room_guid,topic,topic_list,control_event)
                    else:
                        yield self.transmitControlComand(meeting_room_guid,data)
        except Exception as e:
            yield logClient.tornadoErrorLog(str(e))
            self.handle_message_task = None

    @gen.coroutine
    def transmitControlComand(self,meeting_room_guid,data):
        for websocketObject in self.websocketObject.clientObjectSet:
            if meeting_room_guid in websocketObject.meeting_room_list:
                websocketObject.sendToClientService(json_dumps(data))

    @gen.coroutine
    def macroControlHandle(self,meeting_room_guid,topic,topic_list,control_event):
        '''
        发送给websocket的数据
        data = {
            'type': 'control',
            'topic': topic,
            'topic_list': topic_list,
            'control_event': control_event
        }
        '''
        port = topic_list[6]
        channel = topic_list[7]
        try:
            port = int(port)
        except:
            return
        try:
            channel = int(channel)
        except:
            return
        macro_object = self.macro_dict.get(meeting_room_guid)
        if macro_object == None:
            return
        for macro_info in macro_object:
            if macro_info.get('port') == port and macro_info.get('channel') == channel:
                if control_event == 'toggle':
                    if macro_info.get('status') == 'on':
                        work_point = 'off'
                    else:
                        work_point = 'on'
                elif control_event == 'on' or control_event == 'off':
                    work_point = control_event
                else:
                    work_point = 'on'
                jobs_list = macro_info.get('jobs').get(work_point)
                for jobs_channel in jobs_list:
                    event = jobs_channel.get('event')
                    value = jobs_channel.get('value')
                    if event == 'wait':
                        yield gen.sleep(value)
                    else:
                        port = jobs_channel.get('port')
                        channel = jobs_channel.get('channel')
                        topic = '/aaiot/{}/send/controlbus/event/{}/{}/{}'.format(meeting_room_guid,event,port,channel)
                        topic_list = ['aaiot',meeting_room_guid,'send','controlbus','event',event,port,channel]
                        control_event = value
                        data = {
                            'type': 'control',
                            'topic': topic,
                            'topic_list': topic_list,
                            'control_event': control_event
                        }
                        yield self.transmitControlComand(meeting_room_guid,data)

    @gen.coroutine
    def handleChannelSync(self,topic,topic_list,data):
        self.my_publish(topic, data)
        yield self.updateSelfChannelStatus(topic,topic_list,data)

    @gen.coroutine
    def updateSelfChannelStatus(self,topic,topic_list,data):
        meeting_room_guid = topic_list[1]
        type = topic_list[5]
        port = topic_list[6]
        channel = topic_list[7]
        meeting_room_object = self.meeting_room_dict.get(meeting_room_guid)
        if meeting_room_object == None:
            return
        try:
            port = int(port)
        except:
            return
        try:
            channel = int(channel)
        except:
            pass
        if type not in ['channel','level','string','matrix']:
            return
        try:
            meeting_room_object[type][port][channel] = data
        except:
            return
        #同步macro状态
        yield self.updateMacroStatus(meeting_room_guid)

    @gen.coroutine
    def updateMeetingRoomDevice(self,meeting_room_device_list):
        '''
        从网关中得到设备,分解成如下结构
        self.meeting_room_dict = {
            '会议室guid':[
                {
                    'channel':[],
                    'level':[],
                    'string':[],
                    'matrix':[]
                }
            ]
        }
        '''
        update_meeting_room_guid_set = set()
        for meeting_room_info in meeting_room_device_list:
            meeting_room_guid = meeting_room_info.get('meeting_room_guid')
            update_meeting_room_guid_set.add(meeting_room_guid)
            device_list = meeting_room_info.get('device_list')
            # device_ = {}
            for device_info in device_list:
                room_id = device_info.get('room_id')
                update_meeting_room_guid_set.add(room_id)
                function_list = device_info.get('function_list')
                if self.meeting_room_dict.get(room_id):
                    pass
                else:
                    self.meeting_room_dict[room_id] = {
                        'channel': [],
                        'level': [],
                        'string': [],
                        'matrix': []
                    }
                for channel_info in function_list:
                    channel_port = channel_info.get('port')
                    channel_channel = channel_info.get('channel')
                    channel_type = channel_info.get('type')
                    channel_feedback = channel_info.get('feedback')
                    string_name = channel_info.get('string_name')

                    if channel_feedback == 'channel' or channel_feedback == 1:
                        channel_status = channel_info.get('channel_value')
                    elif channel_feedback == 'level' or channel_feedback == 2:
                        channel_status = channel_info.get('level_value')
                    elif channel_feedback == 'string' or channel_feedback == 4:
                        channel_status = channel_info.get('string_value')
                    elif channel_feedback == 'matrix' or channel_feedback == 7:
                        channel_status = channel_info.get('matrix_value').get('channel_guid')
                    elif channel_feedback == 'none' or channel_feedback == 6:
                        if channel_type == 'button' or channel_feedback == 0:
                            channel_status = channel_info.get('channel_value')
                        elif channel_type == 'channel' or channel_feedback == 1:
                            channel_status = channel_info.get('channel_value')
                        elif channel_type == 'level' or channel_feedback == 2:
                            channel_status = channel_info.get('level_value')
                        elif channel_type == 'string' or channel_feedback == 4:
                            channel_status = channel_info.get('string_value')
                        elif channel_type == 'matrix' or channel_feedback == 7:
                            # channel_status = channel_info.get('matrix_value').get('channel_guid')
                            channel_status = None
                        else:
                            raise TypeError('通道类型错误')
                    else:
                        raise TypeError('通道类型错误')

                    if channel_feedback == 'channel':
                        while len(self.meeting_room_dict[room_id]['channel']) < channel_port + 1:
                            self.meeting_room_dict[room_id]['channel'].append([])
                        while len(self.meeting_room_dict[room_id]['channel'][channel_port]) < channel_channel + 1:
                            self.meeting_room_dict[room_id]['channel'][channel_port].append([])
                        self.meeting_room_dict[room_id]['channel'][channel_port][channel_channel] = channel_status
                    if channel_feedback == 'level':
                        while len(self.meeting_room_dict[room_id]['level']) < channel_port + 1:
                            self.meeting_room_dict[room_id]['level'].append([])
                        while len(self.meeting_room_dict[room_id]['level'][channel_port]) < channel_channel + 1:
                            self.meeting_room_dict[room_id]['level'][channel_port].append([])
                        self.meeting_room_dict[room_id]['level'][channel_port][channel_channel] = channel_status
                    if channel_feedback == 'string':
                        while len(self.meeting_room_dict[room_id]['string']) < channel_port + 1:
                            self.meeting_room_dict[room_id]['string'].append({})
                        # while len(self.meeting_room_dict[room_id]['string'][channel_port]) < channel_channel + 1:
                        #     self.meeting_room_dict[room_id]['string'][channel_port].append([])
                        # self.meeting_room_dict[room_id]['string'][channel_port][channel_channel] = channel_status
                        self.meeting_room_dict[room_id]['string'][channel_port][string_name] = channel_status

                    if channel_feedback == 'matrix':
                        while len(self.meeting_room_dict[room_id]['matrix']) < channel_port + 1:
                            self.meeting_room_dict[room_id]['matrix'].append([])
                        while len(self.meeting_room_dict[room_id]['matrix'][channel_port]) < channel_channel + 1:
                            self.meeting_room_dict[room_id]['matrix'][channel_port].append([])
                        self.meeting_room_dict[room_id]['matrix'][channel_port][channel_channel] = channel_status

        for meeting_room_guid in update_meeting_room_guid_set:
            yield self.getMacroInfo(meeting_room_guid)

    @gen.coroutine
    def syncMeetingRoomDevice(self,meeting_room_device_list):
        for meeting_room_info in meeting_room_device_list:
            meeting_room_guid = meeting_room_info.get('meeting_room_guid')
            device_list = meeting_room_info.get('device_list')
            #获取macro信息
            macro_list = []
            meeting_room_macro_list = self.macro_dict.get(meeting_room_guid)
            if meeting_room_macro_list == None:
                yield self.getMacroInfo(meeting_room_guid)
            else:
                for macro_info in meeting_room_macro_list:
                    info = {
                        'channel_guid':macro_info.get('channel_guid'),
                        'channel_value':macro_info.get('status')
                    }
                    macro_list.append(info)
                yield self.reportDeviceInfo(meeting_room_guid, device_list, macro_list)


    @gen.coroutine
    def reportDeviceInfo(self,meeting_room_guid,device_list,macro_list):
        topic = '/aaiot/{}/receive/controlbus/event/command/0/device'.format(meeting_room_guid)
        data = {
            "type": "device",
            "devices_list": device_list,
            "marco_list": macro_list,
            "ts": time.time(),
            "status": "ok",
            "processTime": 0
        }
        self.my_publish(topic, json_dumps(data))
        yield logClient.tornadoDebugLog('同步设备,macro 信息给后台')

    @gen.coroutine
    def updateMeetingRoomMacro(self,meeting_room_guid,macro_info_list):
        '''
        self.macro_dict = {
            'meeting_room_guid':[

            ]
            }
        '''
        for macro_info in macro_info_list:
            feedbackCondition = macro_info.get('feedbackCondition')
            str_header = 'self.meeting_room_dict[meeting_room_guid]'
            if feedbackCondition:
                feedbackCondition = feedbackCondition.replace('channel',str_header+"['channel']")
                feedbackCondition = feedbackCondition.replace('level', str_header + "['level']")
                feedbackCondition = feedbackCondition.replace('string', str_header + "['string']")
                feedbackCondition = feedbackCondition.replace('matrix', str_header + "['matrix']")
            macro_info['feedbackCondition'] = feedbackCondition
        self.macro_dict[meeting_room_guid] = macro_info_list

        yield self.updateMacroStatus(meeting_room_guid)

    @gen.coroutine
    def updateMacroStatus(self,meeting_room_guid):
        macro_list = self.macro_dict.get(meeting_room_guid)
        if macro_list == None:
            return
        for macro_info in macro_list:
            feedbackCondition = macro_info.get('feedbackCondition')
            try:
                result = eval(feedbackCondition)
            except Exception as e:
                yield logClient.tornadoErrorLog(str(e))
                result = False
            if result == True:
                new_status = 'on'
            else:
                new_status = 'off'
            if new_status != macro_info.get('status'):
                macro_info['status'] = new_status
                feedback = macro_info.get('feedback')
                port = macro_info.get('port')
                channel = macro_info.get('channel')
                topic = '/aaiot/{}/receive/controlbus/event/{}/{}/{}'.format(meeting_room_guid,feedback,port,channel)
                yield self.my_publish(topic=topic,payload=new_status)
        pass

    @gen.coroutine
    def getMacroInfo(self, meeting_room_guid):
        topic = '/aaiot/{}/receive/controlbus/event/command/0/macro'.format(meeting_room_guid)
        data = ''
        self.my_publish(topic, data)
        yield logClient.tornadoDebugLog('获取macro信息')

    @gen.coroutine
    def sendHeart(self):
        data = time.time()
        for websocketObject in self.websocketObject.clientObjectSet:
            for meeting_room_guid in websocketObject.meeting_room_list:
                topic = '/aaiot/{}/receive/controlbus/system/heartbeat'.format(meeting_room_guid)
                self.my_publish(topic,data)
        self.ioloop.add_timeout(self.ioloop.time()+15,self.sendHeart)
    #todo:保持mosquitto连接
    def keepConnect(self):
        if self.connect_status == False:
            try:
                self.mosquittoClient.reconnect()
            except:
                logClient.debugLog('mqtt重连失败,尝试继续重连')
                self.ioloop.add_timeout(self.ioloop.time() + 5, self.keepConnect)

    #todo：循环维持
    def mosquittoClientLoop(self):
        if self.connect_status == True:
            self.mosquittoClient.loop(timeout=0.005)
            self.ioloop.add_timeout(self.ioloop.time(),self.mosquittoClientLoop)
        else:
            self.mosquittoClient.loop(timeout=0.005)
            self.ioloop.add_timeout(self.ioloop.time()+0.005, self.mosquittoClientLoop)

    #todo：设置心态主题
    def updateHeartTopic(self,heartTopic):
        self.heartTopic = heartTopic

    #todo：设置心跳间隔
    def updateHeartInterval(self,second):
        self.heartInterval = second

    def on_connect(self,client, userdata, flags, rc):
        if rc == 0:
            self.connect_status = True
            logClient.debugLog('mqtt连接成功')
            client.subscribe('/aaiot/mqttService/receive/controlbus/system/heartbeat', qos=QOS)
            client.subscribe('/aaiot/+/send/controlbus/event/button/#'.format(MEETING_ROOM_GUID), qos=QOS)
            client.subscribe('/aaiot/+/send/controlbus/event/channel/#'.format(MEETING_ROOM_GUID), qos=QOS)
            client.subscribe('/aaiot/+/send/controlbus/event/level/#'.format(MEETING_ROOM_GUID), qos=QOS)
            client.subscribe('/aaiot/+/send/controlbus/event/string/#'.format(MEETING_ROOM_GUID), qos=QOS)
            client.subscribe('/aaiot/+/send/controlbus/event/command/#'.format(MEETING_ROOM_GUID), qos=QOS)
            client.subscribe('/aaiot/+/send/controlbus/event/macro/#'.format(MEETING_ROOM_GUID), qos=QOS)
    def on_disconnect(self,client, userdata, rc):
        self.connect_status = False
        logClient.debugLog('mqtt断开连接,启动重连')
        self.ioloop.add_timeout(self.ioloop.time() + 5, self.keepConnect)

    def on_publish(self,client, userdata, mid):
        print('发布回调',mid)

    def on_subscribe(self,client, userdata, mid, granted_qos):
        print('订阅成功')

    def on_unsubscribe(self,client, userdata, mid):
        print('订阅失败')

    def on_log(self,client, userdata, level, buf):
        print('输出日志')

    def TopicToList(self,topic):
        topic_list = topic.split('/')
        while '' in topic_list:
            topic_list.remove('')
        return topic_list
