# -*- coding: utf-8 -*-
import codecs
import gzip
import hashlib
import os
import random
import re
import string
import logging
import subprocess
import threading
import time
import urllib.parse
from contextlib import contextmanager
from datetime import datetime
from unittest.mock import patch
from collections import deque

import execjs
import pandas as pd
import requests
import websocket
from flask import Flask, request, jsonify, render_template, send_file, abort

from protobuf.douyin import *

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger('werkzeug')
app = Flask(__name__)
tasks = {}

danmu_list = deque()
danmu_lock = threading.Lock()
class No200Filter(logging.Filter):
    def filter(self, record):
        # 检查日志消息是否包含 "200" 且不包含错误状态码
        return "200" not in record.getMessage()


# 添加过滤器到 werkzeug 日志记录器
log.addFilter(No200Filter())


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/play')
def play():
    flv_url = urllib.parse.unquote_plus(request.args.get('flv', ''))
    hls_url = urllib.parse.unquote_plus(request.args.get('hls', ''))
    web_name = urllib.parse.unquote_plus(request.args.get('web', 'Play'))
    return render_template('play.html', flv=flv_url, hls=hls_url, web_name=web_name)


@app.route('/create_task', methods=['POST'])
def create_task():
    input_url = request.form['input_url']
    task_id = find_empty_slot()
    douyin_data = DouyinLive_Datas()
    try:
        room_id = douyin_data.get_roomid(input_url)
        if room_id is None:
            return jsonify({"message": "无效的链接请重试！"})
        live_datas = douyin_data.get_live_datas(room_id)
        status = live_datas["status"]
        nickname = live_datas["nickname"]
        check_nickname = douyin_data.check_nickname(tasks, nickname)
        if check_nickname:
            task = DouyinLiveWebFetcher(task_id, live_datas)
            tasks[task_id] = {'task': task, 'input_url': input_url,
                              'start_time': datetime.now().strftime("%Y/%m/%d %H:%M:%S"),
                              'status': '监控开播中~' if status == 4 else '运行中~', 'file': None}
            thread = threading.Thread(target=task.start)
            thread.start()
            if status == 4:
                return jsonify({'message': "ID:{} {}未开播，监控中~".format(task_id, nickname)})
            elif status == 2:
                return jsonify({'message': "ID:{} {}任务已开始".format(task_id, nickname)})
        else:
            return jsonify({'message': "{}任务已存在,无需重复创建！".format(task_id, nickname)})
    except Exception as e:
        logging.error("发生错误：%s", e)
        return jsonify({"message": "发生错误，请重试！"})


@app.route('/del')
def delete_task():
    task_id = request.args.get('id', None)
    logging.info("收到删除任务请求，任务ID：%s", task_id)
    if task_id:
        if task_id in tasks:
            task_info = tasks.get(task_id)
            task_info['status'] = '手动停止'
            task_info['task'].stop()
            del tasks[task_id]
            logging.info("任务 %s 已删除.", task_id)
            return jsonify({"message": "任务 {} 已删除.".format(task_id)})

        else:
            logging.warning("没有找到任务 %s.", task_id)
            return jsonify({"message": "没有 {} 该任务.".format(task_id)})
    else:
        logging.error("删除任务时发生错误，任务ID为空")
        return jsonify({"message": "发生错误，请重试！"})


@app.route('/tasks', methods=['GET'])
def get_tasks():

    serializable_tasks = {
        task_id: {
            'status': task_info.get('status', '未定义状态'),
            'start_time': task_info.get('start_time', None),
            'stop_time': task_info.get('stop_time', None),
            'people_counting': task_info.get('people_counting', None),
            # 'danmu': task_info.get('danmu', None),
            'nickname': task_info.get('nickname', None),
            'share_url': task_info.get('share_url', None),
            'datas_url': task_info.get('datas_url', None),
            'input_url': task_info.get('input_url', None),
            'now_room_id': task_info.get('now_room_id', None),
            'pull_url': task_info.get('pull_url', None),
            'flv_pull_url': task_info.get('flv_pull_url', None),
            'hls_pull_url': task_info.get('hls_pull_url', None),
            'files': task_info.get('files', None)
        }
        for task_id, task_info in tasks.items()
    }

    global danmu_list
    with danmu_lock:
        # 返回最新的弹幕数据，然后清空列表
        danmus = list(danmu_list.copy())
        danmu_list.clear()
        dm_datas={"弹幕": {"dm":danmus}}
        serializable_tasks.update(dm_datas)
    return jsonify(serializable_tasks)


@app.route('/stop_task/<task_id>', methods=['POST'])
def stop_task(task_id):
    logging.info(tasks)
    task_info = tasks.get(task_id)
    if task_info is None:
        logging.error("任务ID:%s 未找到", task_id)
        return jsonify({'message': "任务ID:{task_id} 未找到".format(task_id=task_id)}), 404

    logging.info("收到停止任务请求，任务ID：%s", task_id)
    task_info['status'] = '手动停止'
    task_info['stop_time'] = datetime.now().strftime("%Y/%m/%d %H:%M:%S")
    task_info['task'].stop()

    return jsonify({'message': "ID:{task_id}任务已停止".format(task_id=task_id)})


@app.route('/task/<task_id>')
def task_files(task_id):
    if task_id in tasks:
        task_info = tasks.get(task_id)

        return render_template('files.html', nickname=tasks[task_id]['nickname'], task_id=task_id,
                               files=tasks[task_id]['files'])
    abort(404)


@app.route('/download/<task_id>/<filename>')
def download_file(task_id, filename):
    def clean_excel_file(filepath):
        logging.info("清洗{}数据中...".format(filepath))
        if os.path.exists(filepath):
            df = pd.read_excel(filepath)
            if df.empty:
                logging.info("{}文件为空，无法清洗数据。".format(filepath))
                return
            duplicate_counts = df['弹幕'].value_counts()
            duplicates_to_remove = duplicate_counts[duplicate_counts > 20].index
            df = df[~df['弹幕'].isin(duplicates_to_remove)]
            df.to_excel(filepath, index=False)
            logging.info("清洗{}数据完毕！".format(filepath))
        else:
            logging.info("没有该文件{}".format(filepath))

    logging.info("收到文件下载请求，任务ID：%s，文件名：%s", task_id, filename)
    if task_id in tasks:
        for file_path in tasks[task_id]['files']:
            if filename == os.path.basename(file_path):
                if os.path.exists(file_path):
                    clean_excel_file(file_path)
                    return send_file(file_path, as_attachment=True)
                else:
                    logging.error("文件不存在: %s", file_path)
                    tasks[task_id]['files'].remove(file_path)
                    abort(404)
    logging.error("文件未找到，任务ID：%s，文件名：%s", task_id, filename)
    abort(404)

@contextmanager
def patched_popen_encoding(encoding='utf-8'):
    original_popen_init = subprocess.Popen.__init__

    def new_popen_init(self, *args, **kwargs):
        kwargs['encoding'] = encoding
        original_popen_init(self, *args, **kwargs)

    with patch.object(subprocess.Popen, '__init__', new_popen_init):
        yield

def find_empty_slot():
    """查找并返回下一个可用的空缺位置。"""
    for i in range(1, len(tasks) + 2):  # 检查多于当前长度的一个位置
        if str(i) not in tasks:
            return str(i)
    logging.error("找不到可用的任务插槽")
    return None  # 理论上不应该到达这里

def get_local_files(nickname):
    directory = f'files/{nickname}'
    files_with_content = {'files': []}
    try:
        # 获取所有Excel文件路径
        file_paths = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith(('.xlsx', '.xls'))]
        for file_path in file_paths:
            try:
                # 尝试读取Excel文件
                df = pd.read_excel(file_path)

                # 如果文件不为空，则加入列表，否则删除文件
                if not df.empty:
                    files_with_content['files'].append(file_path)
                else:
                    os.remove(file_path)
                    print(f"删除空文件: {file_path}")

            except Exception as e:
                print(f"异常 {file_path}: {e}")
        # 解析日期并排序
        def extract_date(file_path):
            filename = os.path.basename(file_path)
            try:
                md_str, hm_str = filename.split('_')[1:3]
                date_str = f"{md_str}_{hm_str.split('.')[0]}"
                return datetime.strptime(date_str, "%m-%d_%H-%M")
            except ValueError:
                return datetime.min
        files_with_content['files'].sort(key=extract_date)
    except FileNotFoundError as e:
        print(f"找不到目录: {e}")
    except Exception as e:
        print(f"异常: {e}")
    return files_with_content["files"]


class DouyinLive_Datas:
    def __init__(self):
        self.ttwid = self.get_ttwid()
        self.generate_ms_token = self.get_generate_ms_token()
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "cookie": "ttwid={}; msToken={}; __ac_nonce=0123407cc00a9e438deb4".format(self.ttwid,
                                                                                      self.generate_ms_token),
        }

    @staticmethod
    def get_generate_ms_token(length=107):
        base_str = string.ascii_letters + string.digits + '=_'
        return ''.join(random.choice(base_str) for _ in range(length))

    @staticmethod
    def get_ttwid():
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        }
        try:
            response = requests.get('https://live.douyin.com/', headers=headers)
            response.raise_for_status()
            return response.cookies.get('ttwid')
        except Exception as err:
            logging.error("获取 ttwid 失败: %s", err)
            return None

    def get_requests_proxy(self):
        proxy_list = [
            "http://admin:password@proxy.com:8888",
            "http://admin:password@proxy.com:8888",
            "http://admin:password@proxy.com:8888",
            "http://admin:password@proxy.com:8888",
        ]
        proxy = random.choice(proxy_list)
        proxies = {
            "http": proxy,
            "https": proxy
        }
        logging.info("使用requests代理：%s", proxies)
        return proxies

    def check_nickname(self, data, nickname):
        if not data:  # 检查是否为空字典
            return True
        for key, value in data.items():
            if value.get("nickname") == nickname:
                return False
        return True

    def get_roomid(self, input_url):
        url_match = re.search(r'https?://\S+', input_url)
        if url_match:
            url_match = url_match.group(0)
            try:
                if 'v.douyin.com' in url_match:
                    expanded_url = requests.get(url=url_match, proxies=self.get_requests_proxy()).url
                    room_id = re.findall(r'(\d{19})', expanded_url)[0]
                    return room_id
                elif 'live.douyin.com' in url_match:
                    room_url = re.search(r'live\.douyin\.com/(\d+)', url_match).group(1)
                elif 'www.douyin.com' in input_url and 'follow' in input_url and 'live' in url_match:
                    room_url = re.search(r"https://www\.douyin\.com/follow/live/(\d+)", url_match).group(1)
                else:
                    logging.warning("无效的 Douyin URL！")
                    return None
            except Exception as err:
                logging.error("解析直播间地址失败！{}".format(err))
                return None
        elif input_url.isdigit():
            room_url = input_url
        if room_url:
            get_room_id_url = 'https://live.douyin.com/{}'.format(room_url)
            try:
                response = requests.get(get_room_id_url, headers=self.headers, proxies=self.get_requests_proxy())
                response.raise_for_status()
                room_id = re.search(r'roomId\\":\\"(\d+)\\"', response.text).group(1)
                return room_id if room_id else None
            except Exception as err:
                logging.error("获取 room_id 失败: %s", err)
                return None
        else:
            return None

    def get_live_datas(self, room_id):
        datas_url = 'https://webcast.amemv.com/webcast/room/reflow/info/?type_id=0&live_id=1&app_id=1128&room_id={}'.format(
            room_id)
        try:
            live_datas = requests.get(datas_url, headers=self.headers, proxies=self.get_requests_proxy()).json()
            room_data = live_datas.get('data', {}).get('room', {})
            status = room_data.get('status', None)
            nickname = room_data.get('owner', {}).get('nickname')
            share_url = room_data.get('share_url')
            next_room_id = room_data.get('owner', {}).get('own_room', {}).get('room_ids_str', [None])[0]
            flv_pull_url = room_data.get('stream_url', {}).get('flv_pull_url', {}).get('FULL_HD1', '')
            flv_pull_url = flv_pull_url.replace('http://', 'https://') if isinstance(flv_pull_url,
                                                                                     str) else flv_pull_url
            pull_url = flv_pull_url.replace('http://', 'https://') if isinstance(flv_pull_url, str) else flv_pull_url
            flv_pull_url = urllib.parse.quote_plus(flv_pull_url if flv_pull_url else '')
            hls_pull_url = room_data.get('stream_url', {}).get('hls_pull_url_map', {}).get('FULL_HD1', '')
            hls_pull_url = hls_pull_url.replace('http://', 'https://') if isinstance(hls_pull_url,
                                                                                     str) else hls_pull_url
            hls_pull_url = urllib.parse.quote_plus(hls_pull_url if hls_pull_url else '')
            live_datas = {
                "status": status, "nickname": nickname, "datas_url": datas_url,
                "share_url": share_url, "now_room_id": room_id, "next_room_id": next_room_id,
                "flv_pull_url": flv_pull_url, "hls_pull_url": hls_pull_url, "pull_url": pull_url,
            }
            return live_datas
        except Exception as err:
            logging.error("获取直播数据失败: %s", err)
            return None


class DouyinLiveWebFetcher:
    def __init__(self, task_id, live_datas, batch_size=100):
        logging.info(live_datas)
        self.task_id = task_id
        self.now_room_id = live_datas["now_room_id"]
        self.nickname = live_datas["nickname"]
        self.share_url = live_datas["share_url"]
        self.datas_url = live_datas["datas_url"]
        self.flv_pull_url = live_datas["flv_pull_url"]
        self.hls_pull_url = live_datas["hls_pull_url"]
        self.pull_url = live_datas["pull_url"]
        self.msg_id = '{}_{}'.format(self.nickname, self.task_id)
        self.douyin_datas = DouyinLive_Datas()
        self.ws = None
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = 3
        self.reconnect_delay = 3

        self.batch_size = batch_size
        self.message_queue = deque()
        self.lock = threading.Lock()

    def start(self):
        if tasks[self.task_id]['status'] == "运行中~":
            tasks[self.task_id]['now_room_id'] = self.now_room_id
            tasks[self.task_id]['flv_pull_url'] = self.flv_pull_url
            tasks[self.task_id]['hls_pull_url'] = self.hls_pull_url
            tasks[self.task_id]['pull_url'] = self.pull_url
        tasks[self.task_id]['share_url'] = self.share_url
        tasks[self.task_id]['datas_url'] = self.datas_url
        tasks[self.task_id]['nickname'] = self.nickname
        tasks[self.task_id]['files'] = get_local_files(self.nickname)
        self.create_xlsx_path()
        self._connectWebSocket()

    def stop(self):
        if self.ws:
            tasks[self.task_id]['stop_time'] = datetime.now().strftime("%Y/%m/%d %H:%M:%S")
            if hasattr(self, 'stop_event'):
                self.stop_event.set()
            self._write_messages()
            self.ws.close()
        else:
            tasks[self.task_id]['stop_time'] = datetime.now().strftime("%Y/%m/%d %H:%M:%S")
            if hasattr(self, 'stop_event'):
                self.stop_event.set()
            self._write_messages()

    def restart_connection(self):
        while True:
            if tasks[self.task_id]['status'] == "手动停止":
                logging.info("{}任务手动结束停止循环监控！".format(self.msg_id))
                break
            logging.info("{}开始循环".format(self.msg_id))
            now_status = self.douyin_datas.get_live_datas(self.now_room_id)["status"]
            if now_status == 2:
                if tasks[self.task_id]['status'] == "手动停止":
                    logging.info("{}任务手动结束停止循环监控！".format(self.msg_id))
                    break
                self._connectWebSocket()
            elif now_status == 4:
                if tasks[self.task_id]['status'] == "手动停止":
                    logging.info("{}任务手动结束停止循环监控！".format(self.msg_id))
                    break
                tasks[self.task_id]['status'] = '监控开播中~'
                tasks[self.task_id]['stop_time'] = None
                tasks[self.task_id]['people_counting'] = None
                tasks[self.task_id]['danmu'] = None
                tasks[self.task_id]['hls_pull_url'] = None
                tasks[self.task_id]['flv_pull_url'] = None
                tasks[self.task_id]['pull_url'] = None
                next_room_id = self.douyin_datas.get_live_datas(self.now_room_id)["next_room_id"]
                logging.info("{}_{}".format(self.msg_id, next_room_id))
                next_room_id_status = self.douyin_datas.get_live_datas(next_room_id)["status"]
                if next_room_id_status != None and next_room_id_status == 2 and next_room_id != None:
                    if tasks[self.task_id]['status'] == "手动停止":
                        logging.info("{}任务手动结束停止循环监控！".format(self.msg_id))
                        break
                    logging.info("{}直播开播状态：{}".format(self.msg_id, next_room_id_status))
                    self.create_xlsx_path()
                    datas = self.douyin_datas.get_live_datas(next_room_id)
                    tasks[self.task_id]['now_room_id'] = datas["next_room_id"]
                    tasks[self.task_id]['nickname'] = datas['nickname']
                    tasks[self.task_id]['share_url'] = datas['share_url']
                    tasks[self.task_id]['datas_url'] = datas['datas_url']
                    tasks[self.task_id]['pull_url'] = datas['pull_url']
                    tasks[self.task_id]['flv_pull_url'] = datas['flv_pull_url']
                    tasks[self.task_id]['hls_pull_url'] = datas['hls_pull_url']
                    tasks[self.task_id]['start_time'] = datetime.now().strftime("%Y/%m/%d %H:%M:%S")
                    tasks[self.task_id]['status'] = '运行中~'
                    self.now_room_id = next_room_id
                    logging.info("{}开播了退出循环".format(self.msg_id))
                    self._connectWebSocket()
                    break
                    return True
                else:
                    logging.info("{}还没开播继续循环".format(self.msg_id))
                    time.sleep(600)
            else:
                logging.info("{}还没开播继续循环".format(self.msg_id))
                time.sleep(600)

    def create_xlsx_path(self):
        # 创建文件夹路径
        folder_path = os.path.join('files', self.nickname)
        # 如果文件夹不存在，则创建
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
        # 创建 .xlsx 文件名
        file_name = '{}_{}.xlsx'.format(self.nickname, datetime.now().strftime("%m-%d_%H-%M"))
        # 组合成完整的文件路径
        file_path = os.path.join(folder_path, file_name)
        if 'files' not in tasks[self.task_id]:
            tasks[self.task_id]['files'] = []
        if tasks[self.task_id]['files']:
            tasks[self.task_id]['files'].append(file_path)
        else:
            tasks[self.task_id]['files'] = [file_path]
        return file_path

    @staticmethod
    def generate_signature(wss, script_file='sign.js'):
        params = (
            "live_id,aid,version_code,webcast_sdk_version,"
            "room_id,sub_room_id,sub_channel_id,did_rule,"
            "user_unique_id,device_platform,device_type,ac,"
            "identity"
        ).split(',')
        wss_params = urllib.parse.urlparse(wss).query.split('&')
        wss_maps = {i.split('=')[0]: i.split("=")[-1] for i in wss_params}
        tpl_params = ["{}={}".format(i, wss_maps.get(i, '')) for i in params]
        param = ','.join(tpl_params)
        md5_param = hashlib.md5(param.encode()).hexdigest()
        with codecs.open(script_file, 'r', encoding='utf8') as f:
            script = f.read()
        context = execjs.compile(script)
        ret = context.call('getSign', {'X-MS-STUB': md5_param})
        return ret.get('X-Bogus')

    def _connectWebSocket(self):
        if tasks[self.task_id]['status'] != "手动停止":
            wss = ("wss://webcast5-ws-web-hl.douyin.com/webcast/im/push/v2/?app_name=douyin_web"
                   "&version_code=180800&webcast_sdk_version=1.0.14-beta.0"
                   "&update_version_code=1.0.14-beta.0&compress=gzip&device_platform=web&cookie_enabled=true"
                   "&screen_width=1536&screen_height=864&browser_language=zh-CN&browser_platform=Win32"
                   "&browser_name=Mozilla"
                   "&browser_version=5.0%20(Windows%20NT%2010.0;%20Win64;%20x64)%20AppleWebKit/537.36%20(KHTML,"
                   "%20like%20Gecko)%20Chrome/126.0.0.0%20Safari/537.36"
                   "&browser_online=true&tz_name=Asia/Shanghai"
                   "&cursor=d-1_u-1_fh-7392091211001140287_t-1721106114633_r-1"
                   "&internal_ext=internal_src:dim|wss_push_room_id:{room_id}|wss_push_did:7319483754668557238"
                   "|first_req_ms:1721106114541|fetch_time:1721106114633|seq:1|wss_info:0-1721106114633-0-0|"
                   "wrds_v:7392094459690748497"
                   "&host=https://live.douyin.com&aid=6383&live_id=1&did_rule=3&endpoint=live_pc&support_wrds=1"
                   "&user_unique_id=7319483754668557238&im_path=/webcast/im/fetch/&identity=audience"
                   "&need_persist_msg_count=15&insert_task_id=&live_reason=&room_id={room_id}&heartbeatDuration=0").format(
                room_id=self.now_room_id)
            signature = self.generate_signature(wss)
            wss += "&signature={}".format(signature)
            proxies = [
                {'host': 'proxy.com'},
                {'host': 'proxy.com'},
                {'host': 'proxy.com'},
                {'host': 'proxy.com'},
            ]
            proxy = random.choice(proxies)
            logging.info("{}使用wss代理：{}".format(self.msg_id, proxy))
            headers = {
                "cookie": "ttwid={}".format(self.douyin_datas.get_ttwid()),
                'user-agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            }
            self.ws = websocket.WebSocketApp(wss,
                                             header=headers,
                                             on_open=self._wsOnOpen,
                                             on_message=self._wsOnMessage,
                                             on_error=self._wsOnError,
                                             on_close=self._wsOnClose)
            try:
                self.ws.run_forever(http_proxy_host=proxy['host'],
                                    http_proxy_port='8888',
                                    proxy_type='http',
                                    http_proxy_auth=("admin", "password"),
                                    )
            except Exception as e:
                logging.warning("{},连接错误：{}".format(self.msg_id, e))
                self.stop()
                raise

    def _wsOnOpen(self, ws):
        logging.info("{} WebSocket已连接.".format(self.msg_id))

    def _wsOnMessage(self, ws, message):
        package = PushFrame().parse(message)
        response = Response().parse(gzip.decompress(package.payload))
        if response.need_ack:
            ack = PushFrame(log_id=package.log_id,
                            payload_type='ack',
                            payload=response.internal_ext.encode('utf-8')
                            ).SerializeToString()
            ws.send(ack, websocket.ABNF.OPCODE_BINARY)

        for msg in response.messages_list:
            method = msg.method
            try:
                {
                    'WebcastChatMessage': self._parseChatMsg,
                    'WebcastControlMessage': self._parseControlMsg,
                    'WebcastRoomUserSeqMessage': self._parseRoomUserSeqMsg,
                }.get(method)(msg.payload)
            except Exception:
                pass

    def _wsOnError(self, ws, error):
        logging.warning("{}_wsOnError: {}".format(self.msg_id, error))
        if str(error) == "Connection to remote host was lost.":
            now_status = self.douyin_datas.get_live_datas(self.now_room_id)["status"]
            if now_status == 2:
                if self.reconnect_attempts < self.max_reconnect_attempts:
                    logging.info("{} 尝试重新连接... ({}/{})".format(self.msg_id, self.reconnect_attempts + 1,
                                                                     self.max_reconnect_attempts))
                    time.sleep(self.reconnect_delay)
                    self.reconnect_attempts += 1
                    self._connectWebSocket()
            else:
                logging.info("{}_{}".format(self.msg_id, tasks[self.task_id]['status']))
                if tasks[self.task_id]['status'] != "手动停止":
                    self.restart_connection()
        else:
            logging.info("{}_{}".format(self.msg_id, tasks[self.task_id]['status']))
            if tasks[self.task_id]['status'] != "手动停止":
                self.restart_connection()

    def _wsOnClose(self, ws, *args):
        logging.info("{} WebSocket连接已关闭.".format(self.msg_id))
        logging.info("{}_{}".format(self.msg_id, tasks[self.task_id]['status']))
        if tasks[self.task_id]['status'] != "手动停止":
            self.restart_connection()

    def _parseChatMsg(self, payload):
        # 聊天消息
        message = ChatMessage().parse(payload)
        user_name = message.user.nick_name
        user_id = message.user.id
        content = message.content
        now_time = datetime.now().strftime("%Y/%m/%d %H:%M:%S")
        user_info = {'时间': [now_time], '用户ID': [user_id], '昵称': [user_name], '弹幕': [content]}
        # tasks[self.task_id]['danmu'] = '{}: {}'.format(user_name, content)
        global danmu_list
        if len(danmu_list) <= 5:
            with danmu_lock:
                danmu_list.append('{}: {}'.format(user_name, content))
        # logging.info("{}时间:{},用户ID:{},昵称:{},弹幕:{}".format(self.msg_id,now_time, user_id, user_name, content))
        with self.lock:
            self.message_queue.append(user_info)
            try:
                if len(self.message_queue) >= self.batch_size:
                    self._write_messages()
            except Exception as e:
                logging.error("{}写消息时发生异常: {}".format(self.msg_id, e))

    def _parseControlMsg(self, payload):
        # 直播间状态消息
        message = ControlMessage().parse(payload)
        if message.status == 3:
            logging.info("{} 直播已下播！".format(self.msg_id))
            logging.info("{}_{}".format(self.msg_id, tasks[self.task_id]['status']))
            if tasks.get(self.task_id) != None or tasks[self.task_id]['status'] != "手动停止":
                self.restart_connection()

    def _parseRoomUserSeqMsg(self, payload):
        '''直播间统计'''
        message = RoomUserSeqMessage().parse(payload)
        current = message.total
        total = message.total_pv_for_anchor
        tasks[self.task_id]['people_counting'] = '实时在线: {}, 累计观看: {}'.format(current, total)

    # 负责从缓存中取出弹幕，并调用append_to_excel进行写入。
    def _write_messages(self):
        if len(self.message_queue) == 0:
            logging.info("{}消息队列是空的，没有东西可写".format(self.msg_id))
            return
        try:
            messages_to_write = list(self.message_queue)
            self.message_queue.clear()
        except Exception as e:
            logging.error("{}处理写入Excel错误: {}".format(self.msg_id, e))
        self.append_to_excel(messages_to_write)

    # 接收一个包含多条弹幕的列表user_info_list，将这些弹幕一次性写入Excel。
    def append_to_excel(self, user_info_list):
        filepath = tasks[self.task_id]['files'][-1]
        columns = ['时间', '用户ID', '昵称', '弹幕']
        # 如果文件不存在则创建一个空的DataFrame
        if not os.path.exists(filepath):
            df = pd.DataFrame(columns=columns)
        else:
            df = pd.read_excel(filepath)
        # 处理传入的批量数据
        for user_info in user_info_list:
            new_row = pd.DataFrame(user_info)
            df = pd.concat([df, new_row], ignore_index=True)
        # 写入Excel
        df.to_excel(filepath, index=False)


if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=65000)