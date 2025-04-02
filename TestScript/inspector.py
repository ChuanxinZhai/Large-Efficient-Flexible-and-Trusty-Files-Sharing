# DO NOT RUN THIS CODE. DO NOT MODIFY THIS CODE.
# Do not run this code. Do not modify this code.
# 不要运行这段代码，也不要改动这段代码。

from socket import *
import argparse
import os
import subprocess
from subprocess import *
import time
import threading
import json, struct
import sys
import multiprocessing as mp
import random
from os.path import join, exists, getsize, splitext, basename, dirname, isdir
import shutil
import signal

storage_folder = '/home/tc/workplace/files'
share_folder = '/home/tc/workplace/cw1/share'
cw1_folder = '/home/tc/workplace/cw1'


def make_package(d, b=None):
    j = json.dumps(dict(d), ensure_ascii=False)
    j_len = len(j)
    if b is None:
        return struct.pack('!II', j_len, 0) + j.encode()
    else:
        return struct.pack('!II', j_len, len(b)) + j.encode() + b


def _argparse():
    parser = argparse.ArgumentParser(description="")
    parser.add_argument('--ip', action='store', required=True, dest='ip', help='ip')
    parser.add_argument('--port', action='store', required=True, dest='port', help='port')
    parser.add_argument('--name', action='store', required=True, dest='name', help='name')
    return parser.parse_args()


def get_process_id(name):
    child = subprocess.Popen(["pgrep", "-f", name], stdout=subprocess.PIPE, shell=False)
    response = child.communicate()[0]
    rs = response.decode().strip().split('\n')
    ars = []
    for r in rs:
        if r != '':
            ars.append(r)
    return ars


def create_files():
    os.makedirs(f'{storage_folder}', exist_ok=True)
    random.seed(0)

    file_info = {}
    with open(join(storage_folder, 'file1.bin'), 'wb') as fid:
        fid.write(os.urandom(10 * 1024 * 1024))

    p = subprocess.Popen(['md5sum', join(storage_folder, 'file1.bin')], stdout=PIPE)
    p.wait()
    md5_file1 = p.stdout.read().decode().split(' ')[0]

    file_info['file1.bin'] = {'md5': md5_file1, 'size': os.path.getsize(join(storage_folder, 'file1.bin'))}

    with open(join(storage_folder, 'file2.ppt'), 'wb+') as fid:
        fid.write(os.urandom(1024))
        fid.seek(500 * 1024 * 1024 - 1)
        fid.write(b'\0')

    p = subprocess.Popen(['md5sum', join(storage_folder, 'file2.ppt')], stdout=PIPE)
    p.wait()
    md5_file2 = p.stdout.read().decode().split(' ')[0]

    file_info['file2.ppt'] = {'md5': md5_file2, 'size': os.path.getsize(join(storage_folder, 'file2.ppt'))}

    md5_folder = []

    file_info['folders'] = {}
    os.makedirs(join(storage_folder, 'folders'), exist_ok=True)
    for i in range(50):
        with open(join(storage_folder, 'folders', f'fxx_{i}.txt'), 'wb') as fid:
            fid.write(os.urandom(1024))
        p = subprocess.Popen(['md5sum', join(storage_folder, 'folders', f'fxx_{i}.txt')], stdout=PIPE)
        p.wait()
        this_file_md5 = p.stdout.read().decode().split(' ')[0]
        md5_folder.append(this_file_md5)
        file_info['folders'][f'fxx_{i}.txt'] = {'md5': this_file_md5, 'size': os.path.getsize(join(storage_folder, 'folders', f'fxx_{i}.txt'))}

    return file_info


def getmd5(path):
    p = subprocess.Popen(['md5sum', path], stdout=PIPE)
    p.wait()
    this_file_md5 = p.stdout.read().decode().split(' ')[0]
    return this_file_md5


def clean_share_folder():
    os.system(f'rm -r {share_folder}')


def get_tcp_package(conn):
    bin_buffer = b''
    while len(bin_buffer) < 8:
        data_rec = conn.recv(8)
        if data_rec == b'':
            time.sleep(0.01)
        if data_rec == b'':
            return None, None
        bin_buffer += data_rec
    data = bin_buffer[:8]
    bin_buffer = bin_buffer[8:]
    j_len, b_len = struct.unpack('!II', data)
    while len(bin_buffer) < j_len:
        data_rec = conn.recv(j_len)
        if data_rec == b'':
            time.sleep(0.01)
        if data_rec == b'':
            return None, None
        bin_buffer += data_rec
    j_bin = bin_buffer[:j_len]
    d = json.loads(j_bin.decode())
    bin_buffer = bin_buffer[j_len:]
    while len(bin_buffer) < b_len:
        data_rec = conn.recv(b_len)
        if data_rec == b'':
            time.sleep(0.01)
        if data_rec == b'':
            return None, None
        bin_buffer += data_rec
    return d, bin_buffer


def tcp_listener(server_port, state, file_info):
    server_socket = socket(AF_INET, SOCK_STREAM)
    server_socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    server_socket.bind(('', server_port))
    server_socket.listen(20)
    # print(f'## Start the TCP service for {state["name"]}\n')
    while True:
        connection_socket, addr = server_socket.accept()
        th = threading.Thread(target=sub_connection, args=(connection_socket, state, file_info))
        th.daemon = True
        th.start()


def move_file(filename):
    if exists(join(storage_folder, filename)) is False:
        return -1

    if exists(join(share_folder, filename)) is True:
        if isdir(join(share_folder, filename)) is True:
            shutil.rmtree(join(share_folder, filename), ignore_errors=True)
        else:
            os.remove(join(share_folder, filename))

    os.makedirs(share_folder, exist_ok=True)

    shutil.move(join(storage_folder, filename), join(share_folder, filename))
    return 0


def check_file(filename, info, timeout):
    t = time.time()
    while True:
        if time.time() - t > timeout:
            rate = 0
            if filename == 'folders':
                file_list = list(info.keys())
                file_list.sort()
                file_number = 0
                for f in file_list:
                    if exists(join(share_folder, 'folders', f)):
                        if getsize(join(share_folder, 'folders', f)) == info[f]['size']:
                            if getmd5(join(share_folder, 'folders', f)) == info[f]['md5']:
                                file_number += 1
                rate = file_number / len(file_list)
            else:
                rate = 0
                if exists(join(share_folder, filename)):
                    rate = getsize(join(share_folder, filename)) / info['size']
            if rate == 0:
                return 9999999
            else:
                return timeout / rate
        else:
            rate = 0
            if filename == 'folders':
                file_list = list(info.keys())
                file_list.sort()
                file_number = 0
                for f in file_list:
                    if exists(join(share_folder, 'folders', f)):
                        if getsize(join(share_folder, 'folders', f)) == info[f]['size']:
                            if getmd5(join(share_folder, 'folders', f)) == info[f]['md5']:
                                file_number += 1
                rate = file_number / len(file_list)
            else:
                if exists(join(share_folder, filename)):
                    if getsize(join(share_folder, filename)) == info['size']:
                        if getmd5(join(share_folder, filename)) == info['md5']:
                            rate = 1
                        else:
                            time.sleep(1)
            if rate == 1:
                return time.time() - t
        time.sleep(0.05)


def sub_connection(connection_socket, state, file_info):
    while True:
        d, b = get_tcp_package(connection_socket)

        if d is None:
            break

        if 'cmd' in d.keys():
            cmd = d['cmd']

            if cmd == 'hello':
                connection_socket.send(make_package(file_info))
                state['ip'] = d['ip']
                # print(state['name'], state['ip'])

            # 关闭程序：打开的程序和本程序
            if cmd == 'end':
                if 'run' in state.keys():
                    pids = get_process_id(f'python3 main.py --ip {state["ip"]}')
                    for pid in pids:
                        os.kill(int(pid), signal.SIGKILL)

                state['system'] = False

            # 移动文件， file name, folder name
            if cmd == 'move':
                if 'filename' in d.keys():
                    move_file(d['filename'])
                    connection_socket.send(make_package({'msg': f'move {d["filename"]} successfully'}))
                else:
                    connection_socket.send(make_package({'msg': 'no this file'}))

            # 检查md5
            if cmd == 'check':
                if 'filename' in d.keys():
                    timeused = check_file(d['filename'], d['info'], d['timeout'])
                    connection_socket.send(make_package({'msg': f'check {d["filename"]} successfully', 'timeused': timeused}))
                else:
                    connection_socket.send(make_package({'msg': 'no this file'}))

            # 更新部分文件
            if cmd == 'update':
                if 'filename' in d.keys():
                    if exists(join(share_folder, d['filename'])):
                        with open(join(share_folder, d['filename']), 'rb+') as fid:
                            fid.write(b'123456789')
                        connection_socket.send(make_package({'msg': 'update', 'md5': getmd5(join(share_folder, d['filename']))}))
                    else:
                        connection_socket.send(make_package({'msg': 'no this file'}))
                else:
                    connection_socket.send(make_package({'msg': 'no this file'}))


            if cmd == 'run':#stdout=PIPE, stderr=PIPE,
                subprocess.Popen(f'python3 main.py --ip {state["ip"]}', shell=True, close_fds=True)
                state['run'] = f'python3 main.py --ip {state["ip"]}'
                connection_socket.send(make_package({'msg': 'run'}))

            if cmd == 'check_run':
                if 'run' in state.keys():
                    pids = get_process_id(state['run'])
                    if len(pids) > 0:
                        connection_socket.send(make_package({'msg': 'running'}))
                    else:
                        connection_socket.send(make_package({'msg': 'not running'}))
                else:
                    connection_socket.send(make_package({'msg': 'not running'}))

            if cmd == 'kill':
                if 'run' in state.keys():
                    pids = get_process_id(state['run'])
                    for pid in pids:
                        os.kill(int(pid), signal.SIGKILL)
                    time.sleep(0.1)
                    del state['run']
                connection_socket.send(make_package({'msg': f'{state["name"]} is killed'}))


if __name__ == '__main__':
    parser = _argparse()
    host_ip = parser.ip
    host_port = parser.port
    host_name = parser.name

    print('Host name:', host_name)

    file_info = create_files()

    state = mp.Manager().dict({'system': True})
    state['name'] = host_name
    p_tcp = mp.Process(target=tcp_listener, args=(int(host_port), state, file_info, ))
    p_tcp.daemon = True
    p_tcp.start()

    while state['system']:
        time.sleep(0.2)
    sys.exit(0)
