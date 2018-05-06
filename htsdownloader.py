# -*- coding: utf-8 -*-
# @Time    : 2018/5/6 上午11:10
# @Author  : alpface
# @Email   : xiaoyuan1314@me.com
# @File    : htsdownloader.py
# @Software: PyCharm

from gevent import monkey
monkey.patch_all()
from gevent.pool import Pool
from gevent import spawn
import requests
from requests import adapters
from urllib.parse import urlparse, urljoin
import os
import time
import settings
import hashlib

USER_AGENT = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36'

class HTSDownloader(object):
    def __init__(self, pool_size, retry=3):
        self.pool = Pool(pool_size)
        self.session = self._init_http_session(pool_size, pool_size, retry)
        # 失败重试次数
        self.retry = retry
        # 本地保存的文件夹
        self.dir = ''
        self.succed = {}
        self.failed = []
        # ts片段的总数量
        self.ts_total = 0


    def _init_http_session(self, pool_connections, pool_maxsize, max_retries):
        session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(pool_connections=pool_connections, pool_maxsize=pool_maxsize, max_retries=max_retries)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        return session

    def run(self, m3u8_url, dir=''):
        if len(dir) == 0:
            dir = os.path.join(settings.base_dir, 'm3u8/'+genearteMD5(m3u8_url))
        self.dir = dir

        if self.dir and not os.path.isdir(self.dir):
            os.makedirs(self.dir)
        headers = {'user-agent': USER_AGENT}
        r = self.session.get(m3u8_url, timeout=10, headers=headers)
        if r.ok:
            response_str = r.content.decode()
            if response_str:
                describe = 'index.txt'
                with open(os.path.join(self.dir, describe), 'w') as f:
                    f.write(response_str)
                ts_list = [urljoin(m3u8_url, n.strip()) for n in response_str.split('\n') if n and not n.startswith('#')]
                # zip函数接受任意多个可迭代对象作为参数,将对象中对应的元素打包成一个tuple,然后返回一个可迭代的zip对象.
                ts_list = list(zip(ts_list, [n for n in range(len(ts_list))]))
                if ts_list:
                    self.ts_total = len(ts_list)
                    print('共有%s个ts片段'%self.ts_total)
                    gev = spawn(self._merge_file)
                    self._download(ts_list)
                    gev.join()
        else:
            print(r.status_code)

    def _download(self, ts_list):
        self.pool.map(self._worker_func, ts_list)
        if self.failed:
            ts_list = self.failed
            self.failed = []
            self._download(ts_list)

    def _worker_func(self, ts_tuple):
        url = ts_tuple[0]
        index = ts_tuple[1]
        retry = self.retry
        while retry:
            try:
                r = self.session.get(url, timeout=20)
                if r.ok:
                    file_name = url.split('/')[-1].split('?')[0]
                    with open(os.path.join(self.dir, file_name), 'wb') as f:
                        f.write(r.content)
                    self.succed[index] = file_name
                    print('片段{name}下载完成'.format(name=file_name))
            except Exception as e:
                print('片段{url}下载失败，错误:{ext}'.format(url=url, ext=str(e)))
                retry -= 1
                self.failed.append((url, index))

    def _merge_file(self):
        '''
        将所有的视频合并成单个完整的视频
        :return:
        '''
        index = 0
        outfile = ''
        while index < self.ts_total:
            file_name = self.succed.get(index, '')
            if file_name:
                infile = open(os.path.join(self.dir, file_name), 'rb')
                if not outfile:
                    outfile = open(os.path.join(self.dir, file_name.split('.')[0] + 'total.' + file_name.split('.')[-1]), 'wb')
                outfile.write(infile.read())
                infile.close()
                os.remove(os.path.join(self.dir, file_name))
                index += 1
            else:
                time.sleep(1)
        if outfile:
            outfile.close()

def genearteMD5(str):
    m = hashlib.md5()
    m.update(str.encode(encoding='utf-8'))
    return  m.hexdigest()

if __name__ == '__main__':
    downloader = HTSDownloader(50)
    downloader.run(settings.m3u8_url)


