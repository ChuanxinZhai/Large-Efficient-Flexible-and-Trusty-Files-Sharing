# Step 1: copy your codes (.py files) to "your_code" folder
# Step 2: Run two virtual machines using TinyCore Linux (the image of the virtual machine is provided)
# Step 3: Install "paramiko" module (If you don't know how to use "pip", check https://box.xjtlu.edu.cn/f/c69fd18ee58e41cf991c/)
# Step 4: Modify two IP addresses of these Linux virtual machines. (Type ifconfig in Terminal)
# Step 5: Run this script for testing your code

# MODIFY IP HERE 修改这里的IP！！！(for step 4)
PC_A_IP = ('192.168.56.18', 8001)  # 8001 is not for you! 8001 端口不是给你用的。
PC_B_IP = ('192.168.56.19', 8001)

marking_result = {
    'RUN_A': False,
    'RUN_B': False,
    'MD5_1B': False,
    'TC_1B': 9999,
    'MD5_2A': False,
    'TC_2A+TC_FA': 9999,
    'MD5_FA': False,
    'MD5_2B': False,
    'TC_2B': 9999
}

from os.path import join, isfile
import json, struct, sys, os, time
import shutil, threading
from socket import *


def traverse(dir_path):
    file_list = []
    file_folder_list = os.listdir(dir_path)
    for file_folder_name in file_folder_list:
        if isfile(join(dir_path, file_folder_name)):
            if file_folder_name[0] != '.':
                file_list.append(join(dir_path, file_folder_name))
        else:
            file_list.extend(traverse(join(dir_path, file_folder_name)))
    return file_list


def run_inspector(file_list, remote_ip, local_ip, local_port, vmx, root_dir):
    py_files = ['inspector.py']
    py_files.extend(file_list)
    remote_python_interpreter = '/usr/local/bin/python3'
    remote_current_working_directory = '/home/tc/workplace/cw1'
    remote_ip = remote_ip
    remote_username = 'tc'
    remote_password = '123'

    from os.path import join
    from paramiko import SSHClient
    from paramiko import AutoAddPolicy
    import threading

    ssh = SSHClient()
    ssh.set_missing_host_key_policy(AutoAddPolicy())
    try:
        ssh.connect(remote_ip, username=remote_username, password=remote_password, port=22, timeout=5)

        # mount /mnt/sda1 to home folder as "workplace"
        ssh.exec_command(
            f'if [ ! -d "workplace" ]; then\nmkdir -p workplace\necho {remote_password} | sudo mount /mnt/sda1 ~/workplace\nfi')
        # change the ownership of workplace folder
        ssh.exec_command(f'echo {remote_password} | sudo chown tc /home/tc/workplace')
        # make the CWD
        ssh.exec_command(f'rm -rf {remote_current_working_directory}')

        time.sleep(1)

        ssh.exec_command(f'mkdir -p {remote_current_working_directory}')
        ssh.exec_command(f'mkdir -p {remote_current_working_directory}/share')

        sftp = ssh.open_sftp()
        for f in py_files:
            components = f.replace(root_dir, '').split('/')
            if len(components) > 1:  # Files in folders
                target_dir = join(remote_current_working_directory, '/'.join(components[:-1])).replace('\\', '/')
                ssh.exec_command(f'mkdir -p {target_dir}')
            else:  # Files in CWD
                target_dir = remote_current_working_directory.replace('\\', '/')
            print(f'Send {components[-1]} to {remote_ip}:{target_dir}')
            sftp.put(f, join(target_dir, components[-1]).replace('\\', '/'))

        if len(py_files) > 0:
            # the first file will be the main file
            main_py = join(remote_current_working_directory, py_files[0]).replace('\\', '/')
            print(f'###################### RUN {py_files[0]} ######################')
            # execute the code
            ssh.exec_command(f'cd {remote_current_working_directory};rm -rf file_dict* log/* share/vma* share/vmc*')

            stdin, stdout, stderr = ssh.exec_command(
                f'cd {remote_current_working_directory}; {remote_python_interpreter} {main_py} --ip={local_ip} --port={local_port} --name={vmx}',
                bufsize=1, get_pty=True)

            stdout_iter = iter(stdout.readline, '')
            stderr_iter = iter(stderr.readline, '')

            def print_line(it):
                for out in it:
                    if out:
                        print(out.strip())

            # multi threads to print output and error
            th_out = threading.Thread(target=print_line, args=(stdout_iter,))
            th_err = threading.Thread(target=print_line, args=(stderr_iter,))

            th_out.start()
            th_err.start()
            th_out.join()
            th_err.join()

            exit_code = stderr.channel.recv_exit_status()
            print(f'###################### EXIT Code {exit_code} ######################')
        else:
            print('No py files.')

    except Exception as ex:
        print(ex)
        sftp.close()
        ssh.close()
        return -1

    sftp.close()
    ssh.close()


def make_package(d, b=None):
    j = json.dumps(dict(d), ensure_ascii=False)
    j_len = len(j)
    if b is None:
        return struct.pack('!II', j_len, 0) + j.encode()
    else:
        return struct.pack('!II', j_len, len(b)) + j.encode() + b


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


def socket_snb(ip, msg):
    try:
        client_socket = socket(AF_INET, SOCK_STREAM)
        # client_socket.settimeout(1)
        client_socket.connect(ip)
        client_socket.send(make_package(msg))
        d, _ = get_tcp_package(client_socket)
        return d
    except Exception as ex:
        time.sleep(0.2)
        return None


if __name__ == '__main__':
    code_list1 = traverse('your_code')
    code_list = [c.replace('\\', '/') for c in code_list1]
    print('Your code:', code_list)


    # Start to run the inspector code on the remote virtual machine
    inspector_th1 = threading.Thread(target=run_inspector, args=(code_list, PC_A_IP[0], '192.168.56.1', PC_A_IP[1], 'PC_A_IP', 'your_code/', ))
    inspector_th1.daemon = True
    inspector_th1.start()

    inspector_th2 = threading.Thread(target=run_inspector, args=(code_list, PC_B_IP[0], '192.168.56.1', PC_B_IP[1], 'PC_B_IP', 'your_code/', ))
    inspector_th2.daemon = True
    inspector_th2.start()

    while True:
        file_info_pca = socket_snb(PC_A_IP, {'cmd': 'hello', 'ip': f'{PC_B_IP[0]}'})
        if file_info_pca is not None:
            print('Response from PC_A: ', file_info_pca)
            break

    while True:
        file_info_pcb = socket_snb(PC_B_IP, {'cmd': 'hello', 'ip': f'{PC_A_IP[0]}'})
        if file_info_pcb is not None:
            print('Response from PC_B: ', file_info_pcb)
            break

    print('\n\n**Have linked to PC_A and PC_B. Ready to test.')

    print('**** PHASE 1 ****')
    print('Start to run your code on PC_A')
    socket_snb(PC_A_IP, {'cmd': 'run'})
    time.sleep(1)

    response_pca = socket_snb(PC_A_IP, {'cmd': 'check_run'})

    if response_pca['msg'] == 'not running':
        print('Testing Error: Code cannot run')
        print('Result', marking_result)
        sys.exit(0)

    print('**** PASS PHASE 1 ****')
    marking_result['RUN_A'] = True
    marking_result['RUN_B'] = True

    print('\n\n**** PHASE 2 ****')
    print('Move file1.bin (File_1 in the handbook) on PC_A to the share folder.')
    socket_snb(PC_A_IP, {'cmd': 'move', 'filename': 'file1.bin'})
    time.sleep(0.1)

    print('Start to run your code on PC_B')
    socket_snb(PC_B_IP, {'cmd': 'run'})

    r = socket_snb(PC_B_IP, {'cmd': 'check', 'filename': 'file1.bin', 'info': file_info_pca['file1.bin'], 'timeout': 10})

    if r['timeused'] > 10:  # Timeout means MD5 check failed (multiplex the "timeused" param)
        # 这里在检查器里其实做过了MD5检查，只是通过 timeused 这一个量同时传递了时间和MD5信息
        # 如果MD5检查错误，时间会自动设置为远超过超时时间的一个值
        print('Testing Error： MD5_1B FAILED')
        marking_result['MD5_1B'] = False
        print('Result', marking_result)
        sys.exit(0)
    else:
        print('MD5_1B: PASS')
        marking_result['MD5_1B'] = True

    marking_result['TC_1B'] = r['timeused']

    print('\n\n**** PHASE 3 ****')
    print('Move file2.ppt (File_2 in the handbook) and folder with 50 files to share folder on PC_B')
    socket_snb(PC_B_IP, {'cmd': 'move', 'filename': 'file2.ppt'})
    socket_snb(PC_B_IP, {'cmd': 'move', 'filename': 'folders'})
    time.sleep(0.5)
    print('Kill your code on PC_A')
    print(socket_snb(PC_A_IP, {'cmd': 'kill'})['msg'])

    time.sleep(1)
    print('Restart PC_A')
    socket_snb(PC_A_IP, {'cmd': 'run'})

    r = socket_snb(PC_A_IP, {'cmd': 'check', 'filename': 'file2.ppt', 'info': file_info_pcb['file2.ppt'], 'timeout': 60})

    if r['timeused'] > 60:
        print('Testing Error：MD5_2A FAILED')
        marking_result['MD5_2A'] = False
        print('Result', marking_result)
        sys.exit(0)
    else:
        print('MD5_2A: PASS')
        marking_result['MD5_2A'] = True
    TC_2A = r['timeused']

    r = socket_snb(PC_A_IP, {'cmd': 'check', 'filename': 'folders', 'info': file_info_pcb['folders'], 'timeout': 60})
    if r['timeused'] > 60:
        print('Testing Error：MD5_2F FAILED')
        marking_result['MD5_FA'] = False
        print('Result', marking_result)
        sys.exit(0)
    else:
        print('MD5_FA: PASS')
        marking_result['MD5_FA'] = True
    TC_FA = r['timeused']

    marking_result['TC_2A+TC_FA'] = TC_2A + TC_FA

    r = socket_snb(PC_A_IP, {'cmd': 'update', 'filename': 'file2.ppt'})
    file_info_pca['file2.ppt']['md5'] = r['md5']

    r = socket_snb(PC_B_IP, {'cmd': 'check', 'filename': 'file2.ppt', 'info': file_info_pca['file2.ppt'], 'timeout': 50})

    if r['timeused'] > 50:
        print('Testing Error: MD5_2B FAILED')
        marking_result['MD5_2B'] = 0
    else:
        print('MD5_2B: PASS')
        marking_result['MD5_2B'] = 1

    marking_result['TC_2B'] = r['timeused']

    print('Result:', marking_result)