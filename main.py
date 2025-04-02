# coding:utf-8

import hashlib, socket, argparse, os, time, math, struct, tqdm
from threading import Thread

block_size = 1024 * 1024 * 5
suffix = ".downloading"
port1 =22223
port2 =22233

'''
https://docs.python.org/zh-cn/3.8/howto/argparse.html#id1 ; https://docs.python.org/zh-cn/3/library/argparse.html#argumentparser-objects
'''
def get_parse():
    arg_parse = argparse.ArgumentParser()  # create a parser
    arg_parse.add_argument('--ip', required=True, help='IP')  # IP
    return arg_parse.parse_args()


def scan_filefolder(filefolder_name):
    if not os.path.exists(filefolder_name):
        os.mkdir(filefolder_name)  # 不存在的话，创文件夹。 not exist, create a folder

    file_list = []
    catologue = os.listdir(filefolder_name)
    for file in catologue:
        if not (len(file) > 11 and file[-11::] == "downloading"):  # 未下载完成 downloading
            file_path=os.path.join(filefolder_name, file)
            if os.path.isfile(file_path):
                file_list.append(file_path)
            else:
                file_list.extend(scan_filefolder(file_path))

    return file_list


def gen_md5(filename, index):  # 更新md5  generate
    f = open(filename, 'rb')
    f.seek(index * block_size)
    content = f.read(block_size)
    f.close()
    return hashlib.md5(content).hexdigest()


from os.path import getsize, getmtime


def get_file_info(file):
    file_name = file.encode()
    md5 = gen_md5(file, 0)
    return struct.pack('!QQd', len(file_name), getsize(file), getmtime(file)) + file_name + md5.encode()


def gen_send_block(info):
    block_index_b = info[:4]
    block_index = struct.unpack('!I', block_index_b)[0]
    fname = info[4:].decode()
    f = open(fname, 'rb')
    f.seek(block_index * block_size)
    read_block = f.read(block_size)
    f.close()
    return struct.pack('!II', block_index, len(read_block)) + read_block  # header  +  body

#   检测
def file_detect():
    socket_Server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    socket_Server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    socket_Server.bind(('', port1))
    socket_Server.listen(8)

    while True:
        socket_, addr_ = socket_Server.accept()
        while True:
            try:
                exist_files = scan_filefolder('share')
                for file in exist_files:
                    send_info = get_file_info(file)
                    socket_.send(send_info)  # 比较文件信息   compare information of file
                    while True:
                        sig_ = socket_.recv(4)
                        sig_ = struct.unpack('!I', sig_)[0]
                        if sig_ == 1:  #文件一样，不发送   same file, not send
                            break
                        else:
                            recv_info = socket_.recv(1024)
                            file_name = recv_info[4:].decode()[6:]
                            if sig_ == 0:  # 0表示没收到     0 means not receive
                                print(f"Synchronizing file name = {file_name} downloading.")
                            elif sig_ == 2: # 2表示没下载（传输）完/打断      2 means downloading/interrupt
                                print(f"Synchronizing file name = {file_name} have interrupted before.")
                            else:   # 3 表示完成了，但需要更新（信息变了）   3 means completed, but need to generate new file
                                print(f"Synchronizing file name = {file_name} have changed.")
                            send_block = gen_send_block(recv_info)
                            socket_.send(send_block)
            except Exception as e:
                print("exception = ", e)
                break


def get_file_detail(recv_info):
    file_name_len, fsize, mtime = struct.unpack('!QQd', recv_info[:24])
    file_name = recv_info[24: 24 + file_name_len]
    md5 = recv_info[24 + file_name_len:].decode()
    return file_name.decode(), fsize, md5, mtime


def check_file(fname, md5, mtime, fsize):
    if not os.path.exists(fname):
        if not os.path.exists(fname + 'downdownloading'):
            file_status = 0  # 不存在     not exist
        else:
            file_status = 2  # 传输终止的   interrupt
    else:
        old_md5 = gen_md5(fname, 0)
        old_size = os.path.getsize(fname)
        if old_md5 == md5 and old_size == fsize:
            file_status = 1  # 传好了的（已经有了）  same file, is OK
        else:
            old_mtime = os.path.getmtime(fname)
            if old_mtime < mtime or old_size < fsize:
                file_status = 3  # 需更新文件   generate
            else:
                file_status = 1

    return file_status

# status = 0 (not exist)
def load_file(fname, f_size, socket_c, f_status):
    _path, _file = os.path.split(fname)
    if (not _path == ' ') and (not os.path.exists(_path)):
        os.makedirs(_path)
    num_block = math.ceil(f_size / block_size)
    with open(fname + suffix, 'wb') as f:
        f.seek(0)
        f.write(b'0')
    with open(fname + suffix, 'wb') as f:
        print(f"downloading file name = {fname[6:]}...")
        for _b_i in tqdm.tqdm(range(num_block)):
            socket_c.send(struct.pack('!I', f_status))
            _req = struct.pack('!I', _b_i) + fname.encode()
            socket_c.send(_req)
            b_info = socket_c.recv(8)
            _b_i, b_len = struct.unpack('!II', b_info)
            buff = b''
            while len(buff) < b_len:
                buff += socket_c.recv(b_len)
            recv_block = buff[:b_len]

            f.seek(block_size * _b_i)
            f.write(recv_block)

    print("Successfully Download~~")   #下载好了
    os.rename(fname + suffix, fname)
    socket_c.send(struct.pack('!I', 1))

# status = 2 (interrupt)
def go_on_trans_file(fname, f_size, socket_c, f_status):  # 断点续传 download resume
    cur_f_size = os.path.getsize(fname + suffix)
    old_b_i = math.floor(cur_f_size / block_size)
    num_block = math.ceil(f_size / block_size)
    with open(fname + suffix, 'ab') as f:
        print(f"Resuming loaded file->{fname[6:]} now")
        for _b_i in tqdm.tqdm(range(old_b_i, num_block)):
            socket_c.send(struct.pack('!I', f_status))
            _req = struct.pack('!I', _b_i) + fname.encode()
            socket_c.send(_req)
            b_info = socket_c.recv(8)
            _b_i, b_len = struct.unpack('!II', b_info)
            buff = b''
            while len(buff) < b_len:
                buff += socket_c.recv(b_len)
            recv_block = buff[:b_len]

            f.seek(block_size * _b_i)
            f.write(recv_block)

    print("Done completely~!")    # 全下载完了

    os.rename(fname + suffix, fname)
    socket_c.send(struct.pack('!I', 1))

# # status = 3 (generate)
def update_file(fname, f_size, socket_c, f_status):
    num_block = math.ceil(f_size / block_size)
    os.rename(fname, fname + suffix)
    with open(fname + suffix, 'rb+') as f:
        print(f"Updating file->{fname[6:]} now")
        for _b_i in tqdm.tqdm(range(num_block)):
            socket_c.send(struct.pack('!I', f_status))
            _req = struct.pack('!I', _b_i) + fname.encode()
            socket_c.send(_req)
            b_info = socket_c.recv(8)
            _b_i, b_len = struct.unpack('!II', b_info)
            buff = b''
            while len(buff) < b_len:
                buff += socket_c.recv(b_len)
            recv_block = buff[:b_len]

            f.seek(block_size * _b_i)
            f.write(recv_block)

    print("Update done~!")   # 更新好了

    os.rename(fname + suffix, fname)
    socket_c.send(struct.pack('!I', 1))


def file_get(serv_ip, port_s, port_c):
    while True:
        while True:
            try:
                socket_Client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                socket_Client.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                socket_Client.bind(('', port_c))
                socket_Client.connect((serv_ip, port_s))
                break
            except:
                pass

        while True:
            try:
                recv_info, addr = socket_Client.recvfrom(10240)
                fname, fsize, md5, mtime = get_file_detail(recv_info)
                f_status = check_file(fname, md5, mtime, fsize)
                if f_status == 0:
                    _path, _file = os.path.split(fname)
                    load_file(fname, fsize, socket_Client, f_status)
                elif f_status == 1:
                    same_info = struct.pack('!I', f_status)
                    socket_Client.send(same_info)
                elif f_status == 2:
                    _path, _file = os.path.split(fname)
                    go_on_trans_file(fname, fsize, socket_Client, f_status)
                elif f_status == 3:
                    update_file(fname, fsize, socket_Client, f_status)
            except Exception as e:
                print("error in get:", e)
                break


if __name__ == "__main__":
    machine_ip = get_parse().ip
    detect_send_t = Thread(target=file_detect)
    # 检测文件 detect file
    req_recv_t = Thread(target=file_get, args=(machine_ip, port1, port2))
    # 建立连接，接收文件 connect and receive file


    #上面两个线程都需要对比文件，看是不是要进行其它操作 The above two threads both need to compare files to decide whether to do other operations or not.

    while True:
        try:
            detect_send_t.start()
            req_recv_t.start()
        except:
            pass
        time.sleep(0.5)
