import json
from json import JSONDecodeError

import aiohttp
import requests
from tornado import gen, httpclient
from tornado.httputil import url_concat
from utils.my_json import json_dumps

from setting.setting import MY_SQL_SERVER_HOST


class MysqlClient():
    def __init__(self,host='127.0.0.1',port='8002'):
        self.host = host
        self.port = port

    @gen.coroutine
    def asyncTornadoRequest(
            self,
            url: str,
            params: dict = None,
            body: dict = None,
            method: str = 'GET',
            headers: dict = None,
            allow_nonstandard_methods: bool = False
    ):
        if body:
            body_str = json_dumps(body)
        else:
            body_str = None
        if params:
            url = url_concat(url, params)
        request = httpclient.HTTPRequest(
            url=url,
            headers=headers,
            method=method,
            body=body_str,
            allow_nonstandard_methods=allow_nonstandard_methods,
        )
        http = httpclient.AsyncHTTPClient()
        try:
            resp = yield http.fetch(request)
            result = json.loads(resp.body.decode())
        except Exception as e:
            result = {'ret':1,'errmsg':str(e)}
        return result

    def openTransaction(self,database):
        headers = {
            'content-type': 'application/json',
        }
        data = {
            'database':database
        }
        resp = requests.get('http://{}:{}/transaction'.format(self.host,self.port),headers=headers,params=data)
        try:
            result = resp.json()
        except:
            result = {
                'ret': 1,
                'msg': [],
            }
        return result

    def commitTransaction(self,transaction_point):
        headers = {
            'content-type': 'application/json',
        }
        data = {
            'transaction_point':transaction_point
        }
        resp = requests.put('http://{}:{}/transaction'.format(self.host,self.port),json=data,headers=headers)
        try:
            result = resp.json()
        except:
            result = {
                'ret': 1,
                'msg': [],
            }
        return result

    def rollbackTransaction(self,transaction_point):
        headers = {
            'content-type': 'application/json',
        }
        data = {
            'transaction_point':transaction_point
        }
        resp = requests.delete('http://{}:{}/transaction'.format(self.host,self.port),json=data,headers=headers)
        try:
            result = resp.json()
        except:
            result = {
                'ret': 1,
                'msg': [],
            }
        return result

    def selectOne(self,table, data):
        headers = {
            'content-type': 'application/json',
        }
        resp = requests.post('http://{}:{}/selectOne/{}'.format(self.host,self.port, table), data=json_dumps(data),headers=headers)
        try:
            result = resp.json()
        except:
            result = {
                'ret': 1,
                'msg': [],
            }
        return result

    def selectAll(self,table, data):
        headers = {
            'content-type': 'application/json',
        }
        resp = requests.post('http://{}:{}/selectAll/{}'.format(self.host,self.port, table), data=json_dumps(data),headers=headers)
        try:
            result = resp.json()
        except:
            result = {
                'ret': 1,
                'msg': [],
            }
        return result

    def selectOnly(self,table, data):
        headers = {
            'content-type': 'application/json',
        }
        resp = requests.post('http://{}:{}/selectOnly/{}'.format(self.host,self.port, table), data=json_dumps(data),headers=headers)
        try:
            result = resp.json()
        except:
            result = {
                'ret': 1,
                'msg': [],
            }
        return result

    def insertOne(self,table, data):
        headers = {
            'content-type': 'application/json',
        }
        resp = requests.post('http://{}:{}/insertOne/{}'.format(self.host,self.port, table), data=json_dumps(data),headers=headers)
        try:
            result = resp.json()
        except:
            result = {
                'ret': 1,
                'msg': [],
            }
        return result

    def conditionInsertOne(self,table, data):
        headers = {
            'content-type': 'application/json',
        }
        resp = requests.post('http://{}:{}/conditionInsertOne/{}'.format(self.host,self.port, table), data=json_dumps(data),headers=headers)
        try:
            result = resp.json()
        except:
            result = {
                'ret': 1,
                'msg': [],
            }
        return result

    def updateMany(self,table, data):
        headers = {
            'content-type': 'application/json',
        }
        resp = requests.post('http://{}:{}/updateMany/{}'.format(self.host,self.port, table), data=json_dumps(data),headers=headers)
        try:
            result = resp.json()
        except:
            result = {
                'ret': 1,
                'msg': [],
            }
        return result

    def sqlFetchone(self,table, data):
        resp = requests.post('http://{}:{}/sqlFetchone/{}'.format(self.host,self.port, table), data=json_dumps(data))
        try:
            result = json.loads(resp.text)
        except JSONDecodeError as e:
            result = resp.text
        return result

    def sqlSelect(self,table, data):
        resp = requests.post('http://{}:{}/sqlSelect/{}'.format(self.host,self.port, table), data=json_dumps(data))
        try:
            result = json.loads(resp.text)
        except JSONDecodeError as e:
            result = resp.text
        return result

    def sqlExecute(self,table, data):
        resp = requests.post('http://{}:{}/sqlExecute/{}'.format(self.host,self.port, table), data=json_dumps(data))
        try:
            result = json.loads(resp.text)
        except JSONDecodeError as e:
            result = resp.text
        return result

    @gen.coroutine
    def tornadoOpenTransaction(self,database):
        url = 'http://{}:{}/transaction'.format(self.host,self.port)
        method = 'GET'
        headers = {
            'content-type': 'application/json',
        }
        data = {
            'database':database
        }
        result = yield self.asyncTornadoRequest(url, method=method, headers=headers,params=data)
        return result

    @gen.coroutine
    def tornadoCommitTransaction(self,transaction_point):
        data = {
            'transaction_point':transaction_point
        }
        url = 'http://{}:{}/transaction'.format(self.host,self.port)
        method = 'PUT'
        headers = {
            'content-type': 'application/json',
        }
        result = yield self.asyncTornadoRequest(url, method=method, headers=headers,body=data)
        return result

    @gen.coroutine
    def tornadoRollbackTransaction(self,transaction_point):
        data = {
            'transaction_point':transaction_point
        }
        url = 'http://{}:{}/transaction'.format(self.host, self.port)
        method = 'DELETE'
        headers = {
            'content-type': 'application/json',
        }
        result = yield self.asyncTornadoRequest(url, method=method, headers=headers,body=data,allow_nonstandard_methods=True)
        return result

    @gen.coroutine
    def tornadoSelectOne(self,table, data):
        url = 'http://{}:{}/selectOne/{}'.format(self.host,self.port, table)
        method = 'POST'
        headers = {
            'content-type': 'application/json',
        }
        result = yield self.asyncTornadoRequest(url, method=method, headers=headers, body=data)
        return result

    @gen.coroutine
    def tornadoSelectAll(self, table, data):
        url = 'http://{}:{}/selectAll/{}'.format(self.host, self.port, table)
        method = 'POST'
        headers = {
            'content-type': 'application/json',
        }
        result = yield self.asyncTornadoRequest(url, method=method, headers=headers, body=data)
        return result

    @gen.coroutine
    def tornadoSelectOnly(self, table, data):
        url = 'http://{}:{}/selectOnly/{}'.format(self.host, self.port, table)
        method = 'POST'
        headers = {
            'content-type': 'application/json',
        }
        result = yield self.asyncTornadoRequest(url, method=method, headers=headers, body=data)
        return result

    @gen.coroutine
    def tornadoInsertOne(self, table, data):
        url = 'http://{}:{}/insertOne/{}'.format(self.host, self.port, table)
        method = 'POST'
        headers = {
            'content-type': 'application/json',
        }
        result = yield self.asyncTornadoRequest(url, method=method, headers=headers, body=data)
        return result

    @gen.coroutine
    def tornadoConditionInsertOne(self, table, data):
        url = 'http://{}:{}/conditionInsertOne/{}'.format(self.host, self.port, table)
        method = 'POST'
        headers = {
            'content-type': 'application/json',
        }
        result = yield self.asyncTornadoRequest(url, method=method, headers=headers, body=data)
        return result

    @gen.coroutine
    def tornadoUpdateMany(self, table, data):
        url = 'http://{}:{}/updateMany/{}'.format(self.host, self.port, table)
        method = 'POST'
        headers = {
            'content-type': 'application/json',
        }
        result = yield self.asyncTornadoRequest(url, method=method, headers=headers, body=data)
        return result

    @gen.coroutine
    def tornadoSqlFetchone(self, table, data):
        url = 'http://{}:{}/sqlFetchone/{}'.format(self.host, self.port, table)
        method = 'POST'
        headers = {
            'content-type': 'application/json',
        }
        result = yield self.asyncTornadoRequest(url, method=method, headers=headers, body=data)
        return result

    @gen.coroutine
    def tornadoSqlSelect(self, table, data):
        url = 'http://{}:{}/sqlSelect/{}'.format(self.host, self.port, table)
        method = 'POST'
        headers = {
            'content-type': 'application/json',
        }
        result = yield self.asyncTornadoRequest(url, method=method, headers=headers, body=data)
        return result

    @gen.coroutine
    def tornadoSqlExecute(self, table, data):
        url = 'http://{}:{}/sqlExecute/{}'.format(self.host, self.port, table)
        method = 'POST'
        headers = {
            'content-type': 'application/json',
        }
        result = yield self.asyncTornadoRequest(url, method=method, headers=headers, body=data)
        return result

    async def asyncioOpenTransaction(self,database):
        url = 'http://{}:{}/transaction'.format(self.host,self.port)
        headers = {
            'content-type': 'application/json',
        }
        data = {
            'database':database
        }
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers,params=data) as resp:
                if resp.status == 200:
                    result = await resp.json()
                else:
                    result = {
                        'ret': 1,
                        'msg': [],
                        'errmsg': await resp.text(encoding='utf8')
                    }
                return result

    async def asyncioCommitTransaction(self,transaction_point):
        data = {
            'transaction_point':transaction_point
        }
        url = 'http://{}:{}/transaction'.format(self.host, self.port)
        headers = {
            'content-type': 'application/json',
        }
        async with aiohttp.ClientSession() as session:
            async with session.put(url, data=json_dumps(data), headers=headers) as resp:
                if resp.status == 200:
                    result = await resp.json()
                else:
                    result = {
                        'ret': 1,
                        'msg': [],
                        'errmsg': await resp.text(encoding='utf8')
                    }
                return result

    async def asyncioRollbackTransaction(self,transaction_point):
        data = {
            'transaction_point':transaction_point
        }
        url = 'http://{}:{}/transaction'.format(self.host,self.port)
        headers = {
            'content-type': 'application/json',
        }
        async with aiohttp.ClientSession() as session:
            async with session.delete(url, data=json_dumps(data), headers=headers) as resp:
                if resp.status == 200:
                    result = await resp.json()
                else:
                    result = {
                        'ret': 1,
                        'msg': [],
                        'errmsg': await resp.text(encoding='utf8')
                    }
                return result

    async def asyncioSelectOne(self,table, data):
        url = 'http://{}:{}/selectOne/{}'.format(self.host, self.port, table)
        headers = {
            'content-type': 'application/json',
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=json_dumps(data), headers=headers) as resp:
                if resp.status == 200:
                    result = await resp.json()
                else:
                    result = {
                        'ret': 1,
                        'msg': [],
                        'errmsg': await resp.text(encoding='utf8')
                    }
                return result

    async def asyncioSelectAll(self,table, data):
        url = 'http://{}:{}/selectAll/{}'.format(self.host,self.port, table)
        headers = {
            'content-type': 'application/json',
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(url,data = json_dumps(data),headers=headers) as resp:
                if resp.status == 200:
                    result = await resp.json()
                else:
                    result = {
                        'ret':1,
                        'msg':[],
                        'errmsg':await resp.text(encoding='utf8')
                    }
                return result

    async def asyncioSelectOnly(self,table, data):
        url = 'http://{}:{}/selectOnly/{}'.format(self.host,self.port, table)
        headers = {
            'content-type': 'application/json',
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=json_dumps(data), headers=headers) as resp:
                if resp.status == 200:
                    result = await resp.json()
                else:
                    result = {
                        'ret': 1,
                        'msg': [],
                        'errmsg': await resp.text(encoding='utf8')
                    }
                return result

    async def asyncioInsertOne(self,table, data):
        url = 'http://{}:{}/insertOne/{}'.format(self.host,self.port, table)
        headers = {
            'content-type': 'application/json',
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=json_dumps(data), headers=headers) as resp:
                if resp.status == 200:
                    result = await resp.json()
                else:
                    result = {
                        'ret': 1,
                        'msg': [],
                        'errmsg': await resp.text(encoding='utf8')
                    }
                return result

    async def asyncioConditionInsertOne(self,table, data):
        url = 'http://{}:{}/conditionInsertOne/{}'.format(self.host, self.port, table)
        headers = {
            'content-type': 'application/json',
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=json_dumps(data), headers=headers) as resp:
                if resp.status == 200:
                    result = await resp.json()
                else:
                    result = {
                        'ret': 1,
                        'msg': [],
                        'errmsg': await resp.text(encoding='utf8')
                    }
                return result

    async def asyncioUpdateMany(self,table, data):
        url = 'http://{}:{}/updateMany/{}'.format(self.host, self.port, table)
        headers = {
            'content-type': 'application/json',
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=json_dumps(data), headers=headers) as resp:
                if resp.status == 200:
                    result = await resp.json()
                else:
                    result = {
                        'ret': 1,
                        'msg': [],
                        'errmsg': await resp.text(encoding='utf8')
                    }
                return result

    async def asyncioSqlFetchone(self,table, data):
        url = 'http://{}:{}/sqlFetchone/{}'.format(self.host, self.port, table)
        headers = {
            'content-type': 'application/json',
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=json_dumps(data), headers=headers) as resp:
                if resp.status == 200:
                    result = await resp.json()
                else:
                    result = {
                        'ret': 1,
                        'msg': [],
                        'errmsg': await resp.text(encoding='utf8')
                    }
                return result

    async def asyncioSqlSelect(self,table, data):
        url = 'http://{}:{}/sqlSelect/{}'.format(self.host, self.port, table)
        headers = {
            'content-type': 'application/json',
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=json_dumps(data), headers=headers) as resp:
                if resp.status == 200:
                    result = await resp.json()
                else:
                    result = {
                        'ret': 1,
                        'msg': [],
                        'errmsg': await resp.text(encoding='utf8')
                    }
                return result

    async def asyncioSqlExecute(self,table, data):
        url = 'http://{}:{}/sqlExecute/{}'.format(self.host, self.port, table)
        headers = {
            'content-type': 'application/json',
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=json_dumps(data), headers=headers) as resp:
                if resp.status == 200:
                    result = await resp.json()
                else:
                    result = {
                        'ret': 1,
                        'msg': [],
                        'errmsg': await resp.text(encoding='utf8')
                    }
                return result

mysqlClient = MysqlClient(host=MY_SQL_SERVER_HOST)
