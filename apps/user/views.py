import json

from tornado import web, gen

class BaseHanderView(web.RequestHandler):
    def set_default_headers(self) -> None:
        print('调用了set_default_headers')
        self.set_header('Content-Type','application/json;charset=UTF-8')

    def write_error(self, status_code: int, **kwargs) -> None:
        print('调用write_error')
        self.write(u"<h1>出错了</h1>")
        self.write(u'<p>{}</p>'.format(kwargs.get('error_title','')))
        self.write(u'<p>{}</p>'.format(kwargs.get('error_message','')))

    def initialize(self,**kwargs) -> None:
        self.ioloop = kwargs.get('ioloop')
        print('调用initialize')

    def prepare(self):
        if self.request.headers.get('Content-Type','').startswith('application/json'):
            self.json_dict = json.loads(self.request.body)
        else:
            self.json_dict = None

    def on_finish(self) -> None:
        pass
class IndexView(BaseHanderView):
    def test_func(self):
        print('异步调用')

    @gen.coroutine
    def get(self,*args,**kwargs):
        self.ioloop.add_timeout(self.ioloop.time()+5,self.test_func)
        print('调用get')
        # subject = self.get_query_argument('subject')
        # print(subject)
        # subject = self.get_query_arguments('subject')
        # print(subject)
        # subject = self.get_body_argument('subject')
        # print(subject)
        # subject = self.get_body_arguments('subject')title
        # print(subject)
        # subject = self.get_argument('subject')
        # print(subject)
        # subject = self.get_arguments('subject')
        # print(subject)
        s = self.request.arguments
        s = self.request.body
        # s1 = json.loads(s)
        s = self.request.body_arguments
        s = self.request.connection
        s = self.request.files
        for name,file_list in s.items():
            for file_object in file_list:
                filename = file_object['filename']
                body = file_object['body']
                content_type = file_object['content_type']
                with open(filename,'ab') as f:
                    f.write(body)
        s = self.request.headers
        s = self.request.cookies
        s = self.request.full_url()
        # s = self.request.get_ssl_certificate(binary_form=False)
        s = self.request.host
        s = self.request.host_name
        s = self.request.method
        s = self.request.path
        s = self.request.protocol
        s = self.request.query
        s = self.request.query_arguments
        s = self.request.request_time()
        s = self.request.version
        s = self.request.remote_ip
        s = self.request.server_connection
        s = self.request.uri
        info = {
            'name':'pancunli',
            'age':18
        }
        # self.write(info)
        # self.set_header('Content-Type','application/json;charset=UTF-8')
        # self.write(json.dumps(info))
        # self.redirect('https://www.runoob.com/http/http-status-codes.html')     #重定向
        # error_info = {
        #     'error_title':'服务器崩溃了',
        #     'error_message':'服务器累了,现在不能提供服务'
        # }
        # self.send_error(**error_info)
        self.render('index.html')
        # self.write('hello world')
        # self.write('hello world')